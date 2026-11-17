"""Focused tests for Unity Catalog routing and the shared connection API."""

import importlib.util
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# Open-source PySpark does not include Databricks' DBUtils. These tests mock
# credential lookup, so a placeholder lets them run outside Databricks too.
if importlib.util.find_spec('pyspark.dbutils') is None:
    dbutils = types.ModuleType('pyspark.dbutils')
    dbutils.DBUtils = MagicMock()
    sys.modules['pyspark.dbutils'] = dbutils

from data_io.connection import (
    OracleConnection,
    PostgresqlConnection,
    RedshiftConnection,
    S3Connection,
    UCConnection,
    build_connection,
)
from data_io.data_source import build_data_source

UC_TABLE = 'ccg_cdl_federated.marketing_dm.dim_quotations_history_vw'


class UCConnectionTests(unittest.TestCase):
    def test_uc_routing_does_not_resolve_secrets_or_validate_identifier(self):
        """Route UC identifiers without looking up database credentials."""
        with patch('data_io.connection.ConnectionBuilder.get_credentials') as credentials:
            for identifier in (UC_TABLE, 'not..valid'):
                connection = build_connection(identifier)
                self.assertIsInstance(connection, UCConnection)
                self.assertEqual(connection.url, identifier)
            credentials.assert_not_called()

    def test_existing_backend_routing_and_database_credentials(self):
        """Keep URL-based backend routing and credential lookup unchanged."""
        cases = (
            ('redshift://user@host:5439/db.schema.table', RedshiftConnection),
            ('oracle://user@host:1521/db.schema.table', OracleConnection),
            ('postgresql://user@host:5432/db.schema.table', PostgresqlConnection),
            ('s3a://bucket/path', S3Connection),
            ('s3://bucket/path', S3Connection),
        )
        with patch('data_io.connection.ConnectionBuilder.get_credentials', return_value=('actual', 'secret', 'user')) as credentials:
            for url, expected in cases:
                with self.subTest(url=url):
                    self.assertIsInstance(build_connection(url), expected)
            self.assertEqual(credentials.call_count, 3)

    def test_table_and_query_reads_use_spark_and_propagate_errors(self):
        """Pass table identifiers and SQL to Spark and propagate read errors."""
        connection = build_connection(UC_TABLE)
        spark = MagicMock()
        self.assertIs(connection.read(spark=spark), spark.read.table.return_value)
        spark.read.table.assert_called_once_with(UC_TABLE)
        query = f'SELECT a.id FROM {UC_TABLE} a JOIN {UC_TABLE} b ON a.id = b.id'
        self.assertIs(connection.read(spark=spark, query=query), spark.sql.return_value)
        spark.sql.assert_called_once_with(query)
        spark.sql.side_effect = RuntimeError('permission denied')
        with self.assertRaisesRegex(RuntimeError, 'permission denied'):
            connection.read(spark=spark, query='')
        spark.read.table.side_effect = ValueError('invalid identifier')
        with self.assertRaisesRegex(ValueError, 'invalid identifier'):
            build_connection('not..valid').read(spark=spark)

    def test_default_spark_session_is_used(self):
        """Obtain a Spark session when the caller does not provide one."""
        connection = build_connection(UC_TABLE)
        with patch('data_io.connection.SparkSession') as spark_session:
            spark = spark_session.builder.getOrCreate.return_value
            self.assertIs(connection.read(), spark.read.table.return_value)
            spark.read.table.assert_called_once_with(UC_TABLE)

    def test_writes_forward_mode_options_and_errors(self):
        """Forward write modes and options and propagate write failures."""
        connection = build_connection(UC_TABLE)
        data = MagicMock()
        self.assertIsNone(connection.write(data))
        data.write.mode.assert_called_once_with('errorifexists')
        data.write.mode.return_value.options.return_value.saveAsTable.assert_called_once_with(connection.url)
        data.reset_mock()
        connection.write(data, mode='append', mergeSchema='true')
        data.write.mode.assert_called_once_with('append')
        data.write.mode.return_value.options.assert_called_once_with(mergeSchema='true')
        data.write.mode.return_value.options.return_value.saveAsTable.side_effect = RuntimeError('write denied')
        with self.assertRaisesRegex(RuntimeError, 'write denied'):
            connection.write(data, mode='overwrite')

    def test_data_source_checks_filter_rows_and_return_false_on_errors(self):
        """Check filtered data and preserve false-on-error behavior."""
        source = build_data_source(UC_TABLE, None)
        spark = MagicMock()
        frame = spark.read.table.return_value
        with patch('data_io.connection.sf.expr', side_effect=lambda expression: expression):
            frame.filter.return_value.take.return_value = [object()]
            self.assertTrue(source.exists(spark=spark))
            self.assertTrue(source.has_data(spark=spark))
            frame.filter.assert_called_with('TRUE')
            frame.filter.return_value.take.return_value = []
            self.assertTrue(source.exists(spark=spark))
            self.assertFalse(source.has_data(spark=spark))
            filtered = build_data_source(UC_TABLE, 'id > 3')
            filtered.has_data(spark=spark)
            frame.filter.assert_called_with('id > 3')
            frame.filter.side_effect = RuntimeError('invalid filter')
            with self.assertLogs(level='ERROR'):
                self.assertFalse(filtered.has_data(spark=spark))
            frame.filter.side_effect = None
            spark.read.table.side_effect = RuntimeError('missing or denied')
            with self.assertLogs(level='ERROR'):
                self.assertFalse(source.exists(spark=spark))
            with self.assertLogs(level='ERROR'):
                self.assertFalse(source.has_data(spark=spark))

    def test_location_and_jdbc_properties_are_not_supported(self):
        """Report that UC tables do not use URL locations or JDBC URLs."""
        connection = build_connection(UC_TABLE)
        with self.assertRaises(NotImplementedError):
            _ = connection.location
        with self.assertRaises(NotImplementedError):
            _ = connection.jdbc_url


if __name__ == '__main__':
    unittest.main()
