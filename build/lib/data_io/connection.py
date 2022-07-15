"""Tools and builders used for standardized data IO"""
from .location import build_location, DatabaseLocation
from .builder import URLKeyBuilder
import abc
import io
import logging
from attr import attrs, attrib
import urllib
from pyspark.sql import SparkSession, DataFrame, DataFrameReader, DataFrameWriter
import pyspark.sql.functions as sf
from pyspark.dbutils import DBUtils
from configparser import RawConfigParser
from typing import Any, Optional, Tuple, Union
from urlpath import URL


@attrs
class BaseConnection(abc.ABC):
    """
    Describes a basic connection object. Connections minimally require
    a `read` and `write` method.

    Note: Connections should not be initialized directly. Use the builders
    or higher-level APIs instead.
    """

    url: str = attrib(default=None)

    @property
    def location(self):
        """
        Store location information as a location object. For now, we'll
        build these on the fly rather than cacheing. But we'll eventually
        want to cache this information (e.g., lru cache)
        """

        return build_location(self.url)

    @property
    def jdbc_url(self):

        # setting default for port for redshift and oracle
        # if(self.location.port == None):
        #     if(self.location.scheme == 'oracle'):
        #         port = 1521
        #     elif(self.location.scheme == 'redshift'):
        #         port = 5439

        # else:
        #     port = self.location.port

        """
        JDBC connection strings are required to connect to various backends
        such as Oracle, Redshift, Postgres, etc. This provides a default
        JDBC connection string. Specific connections might require modification
        but this should cover the bulk of LSG's use cases.
        """

        return "jdbc:{scheme}://{hostname}:{port}/{db}".format(
            scheme=self.location.scheme,
            hostname=self.location.hostname,
            port=self.location.port,
            # port = port,
            db=self.location.db
        )

    def read(
        self,
        spark=None,
        *largs,
        **kwargs
    ):
        """
        Read data at specificed `location` as a Spark DataFrame

        Args:
          spark (Databricks spark object):
          kwargs: generic read options.

        Returns:
          data (Pyspark DataFrame): The data
        """

        raise NotImplementedError

    def write(
        self,
        data,
        *largs,
        **kwargs
    ):
        """
        All connections will have a write method, but details
        of write will differ depending on the backend.
        """

        raise NotImplementedError

    def _check_source(self, *largs, **kwargs):
        """
        Validates read and catches reasons for failure. Proved useful
        when running source existence and has_data.

        Args:
          largs/kwargs passed through to self.read()

        Returns:
          exists, has_data (tuple)
        """

        n_rows = 0
        exists = False

        try:

            logging.info(f'Verifying {self.url} exists.')

            # This will fail of the table / source doesn't exist
            data_frame = (
              self
              .read(
                *largs,
                **kwargs
              )
            )

            # We only need a single row to confirm existence and data integrity
            # (for now). This can be expanded in time.
            n_rows = len(
              data_frame
              .take(1)
            )

            # If the read works, then the source exists
            exists = True

#         except java.sql.SQLException as e:
#             # Note that this exception handling isn't awesome at the moment.
#             # This will also trigger if there are errors in the SQL statement itself.
#             logging.exception(f'{self.url} does NOT exist.')
#             logging.debug(e)

        except Exception as e:
            
            # Error handling
            logging.info(f'Unexpected Exception. Does your source exist?:\n\n')
            logging.exception(e)

        finally:
            # exists and has_data returned
            has_data = n_rows > 0

            # Print status for debugging
            logging.debug(
                f'{self.url}: Exists: {exists}, Has Data: {has_data}')

        return exists, has_data


    def check_filter(self, filter_str=None, **kwargs):
        """
        Examine the return of connection with specific filter applied.

        Note that small changes are required for S3 (and other non-SQL) reads.

        Args:
          filter_str (str): filter string to test

        Returns:
          exists, has_data
        """

        # Build the query string
        query = f"""
        SELECT
          1
        FROM
          {self.location.schema}.{self.location.table}
        {f'WHERE {filter_str}' if filter_str is not None else ''}
        LIMIT 1"""

        logging.info(f"Checking filter '{filter_str}'")

        # Check the source with a filter in place.
        # We opt to use a SQL query so we push work back to the backend (when possible)
        return self._check_source(query=query)


class ConnectionBuilder(URLKeyBuilder):
    """
    Builds the appropriate connection object from a URL.

    Adding Connections:
      - Write connection class
      - Register with builder
    """

    def __init__(self, *largs, **kwargs):
        super(URLKeyBuilder, self).__init__()

    def build(self, url: str) -> BaseConnection:
        """
        Build a connection and return it
        """

        # Get the key
        url_key = self.build_url_key(url)

        # Generate a location object. This describes where the data are
        location = build_location(url)

        # Let's get the handler so we can do some conditional checks
        connection_class = self.get_handler(url_key)

        # We'll need to expand this check as we add other types of
        # connections that require authentication.
        #
        # At time of writing, the logic here should support JDBC connections
        # like Redshift/Oracle
        if isinstance(location, DatabaseLocation):
            """
            Grab connection information from stored config file.
            """

            # Get credentials
            username, password = self.get_credentials(url)

            # Alter location parts to create a new URL
            # Ran into URL parsing issues, so needed to
            # quote username and password
            _url = (
                url
                .replace(
                    'user@',
                    f'{urllib.parse.quote(username)}:{urllib.parse.quote(password)}@'
                )
            )

        else:

            _url = url

        # Need a little more information (potentially)
        # for Redshift connections (e.g., s3 bucket)
        if isinstance(connection_class, RedshiftConnection):

            pass

        connection = connection_class(_url)

        return connection

    def get_secret(self, key: str, scope: Optional[str] = None) -> str:
        """
        Retrieve a desired key from the specified secrets scope
        """

        if scope is None:

            # Need to get the active spark session
            spark = SparkSession.builder.getOrCreate()

            # Create dbutils object so we can get the user name
            # This enables the package to function outside of a notebook
            dbutils = DBUtils(spark)

            # Reference: https://kb.databricks.com/notebooks/get-notebook-username.html
            # XXX Note: assumes user@ in URL, but this might not be true moving forward
            scope = (
                dbutils
                .notebook
                .entry_point
                .getDbutils()
                .notebook()
                .getContext()
                .tags()
                .apply('user')
                # XXX Hack incoming
                .split('@')
                [0]
            )

        # Retrieve the secret, yo!
        secret = (
            dbutils
            .secrets
            .get(
                scope=scope,
                key=key
            )
        )

        return secret

    def get_credentials(self, url: str) -> Tuple[str, str]:
        """
        Needed a way to do routine credential lookups.

        At time of writing, credentials will be stored as
        databricks secrets registered in a user-specific
        scope. While credentials are often shared
        (e.g., service accounts) this allows us the flexibility
        of using different accounts to improve security.

        Future dev:
        - Default lookup to DSLSG_SCOPE?
        """

        # Figure out the relevant base string for the
        # username and credentials.
        location = build_location(url)

        # Makes formatted strings below nicer to work with
        # XXX Hard-coded user@. Sloppy, Bishop. Sloppy.
        cred_base = f"{location.scheme}://user@{location.hostname}/{location.db}"

        # Let's get the new credentials.cfg
        # XXX Hard-coded credentials file name. Sloppy, Bishop. Sloppy.
        config_file = self.get_secret('credentials.cfg')

        # Convert to a file-like object
        config_buffer = io.StringIO(config_file)

        # Create a parser so we can strip out the information we need
        # 2021-05-11 CWB: changed config parser to RawConfigParser to
        # bypass interpolation.
        # https://stackoverflow.com/questions/14340366/configparser-and-string-with
        config_parser = RawConfigParser()
        config_parser.read_file(config_buffer)

        # Retrieve username
        # Hard-coding OK here.
        username = config_parser.get(cred_base, 'username')
        password = config_parser.get(cred_base, 'password')

        return username, password


class DatabaseConnection(BaseConnection):
    """
      Intermediate connection class with common code and default behavior.
    """

    def check_spark_session(
      self,
      spark: Union[None, SparkSession]
    ) -> SparkSession:
        # check for active spark session
        if spark is None:
            spark = SparkSession.builder.getOrCreate()

        return spark

    def build_options(self, default_options: dict, is_read: bool, **kwargs) -> dict:
        # We'll set the default options then override with
        # whatever the user wants
        options = default_options

        for option, value in kwargs.items():
            options[option] = value

        # This is clunky, but we needed a way to support query/dbtable
        if (
          not(isinstance(self, S3Connection))
          and is_read
          and ('query' not in options.keys() and 'dbtable' not in options.keys())
        ):
            options['query'] = f'SELECT * FROM {self.location.schema}.{self.location.table}'

        return options

    def implement_options(
      self,
      r_w: Union[DataFrameWriter, DataFrameReader],
      options: dict
    ) -> Union[DataFrameWriter, DataFrameReader]:
        # Set options for the reader object
        for option, value in options.items():
            r_w = r_w.option(option, value)

        return r_w

    def build_reader(
        self,
        spark: Optional[SparkSession] = None,
        default_read_options: Optional[dict] = None,
        *largs,
        **kwargs
    ) -> DataFrameReader:

        spark = self.check_spark_session(spark)

        read_options = self.build_options(default_read_options, True, **kwargs)

        reader = spark.read.format(read_options['format'])
        reader = self.implement_options(reader, read_options)

        return reader

    def build_writer(
        self,
        data: DataFrame,
        default_write_options: Optional[dict] = None,
        *largs,
        **kwargs
    ) -> DataFrameWriter:

        write_options = self.build_options(
            default_write_options, False, **kwargs)

        writer = data.write

        # set format and mode separately, remove from options dict
        if 'format' in write_options:
            writer = writer.format(write_options['format'])
            write_options.pop('format', None)

        if 'mode' in write_options:
            writer = writer.mode(write_options['mode'])
            write_options.pop('mode', None)

        writer = self.implement_options(writer, write_options)

        return writer


class RedshiftConnection(DatabaseConnection):
    """
    Redshift connector

    XXX Temporary S3 bucket is hard-coded. It will break with other roles.
    XXX Needs hardening.
    """

    def read(
        self,
        spark: Optional[SparkSession] = None,
        *largs,
        **kwargs
    ) -> DataFrame:
        """
        Read method for redshift data sources.

        Named inputs are assumed to be read parameters

        Args:
          spark (spark session): spark thingy

        Returns:
          data (spark DF): the data, YO
        """

        # Set default read options
        default_read_options: dict = dict(
            user=self.location.username,
            password=self.location.password,
            forward_spark_s3_credentials=True,
            url=self.jdbc_url,
            format="com.databricks.spark.redshift",
            # This is the most questionable to me ... OK to use
            # for multiple people?
            # XXX This should be set through a shared configuration
            # file so other groups can use it
            tempdir='s3a://tfsds-lsg-test/ingestion/redshift_temp',
            # By default, read everything from the table
            # dbtable=f'{self.location.schema}.{self.location.table}'
            # query=f'SELECT * FROM {self.location.schema}.{self.location.table}'
        )

        reader = self.build_reader(
            spark=spark,
            default_read_options=default_read_options,
            *largs,
            **kwargs
        )

        # Finally, load the data and return a pyspark DF
        data = reader.load()

        return data

    def write(
        self,
        data: DataFrame,
        *largs,
        **kwargs
    ) -> None:

        # Set default options
        # XXX Spark's write API is not homogenous,
        # so we're going to force it to be
        default_write_options: dict = dict(
            format="com.databricks.spark.redshift",
            url=self.jdbc_url,
            user=self.location.username,
            password=self.location.password,
            # 2022-02-15 CB: reenabled default
            dbtable=f"{self.location.schema}.{self.location.table}",
            forward_spark_s3_credentials=True,
            tempdir="s3a://tfsds-lsg-test/ingestion/redshift_temp",
            mode='default'
        )

        writer = self.build_writer(
            data,
            default_write_options=default_write_options,
            *largs,
            **kwargs
        )

        writer.save()

        return None


class PostgresqlConnection(DatabaseConnection):
    """
    Postgres

    This needs to be reorganized so Postgres/Redshift and related connections
    have far, far less redundant code.
    """

    def read(
        self,
        spark: Optional[SparkSession] = None,
        *largs,
        **kwargs
    ) -> DataFrame:
        """
        Read method for redshift data sources.

        Named inputs are assumed to be read parameters

        Args:
          spark (spark session): spark thingy

        Returns:
          data (spark DF): the data, YO
        """

        # Set default read options
        default_read_options: dict = dict(
            format='jdbc',
            user=self.location.username,
            driver="org.postgresql.Driver",
            password=self.location.password,
            forward_spark_s3_credentials=True,
            url=self.jdbc_url,
            mode='default'
        )

        reader = self.build_reader(
            spark=spark,
            default_read_options=default_read_options,
            *largs,
            **kwargs
        )

        # Finally, load the data and return a pyspark DF
        data = reader.load()

        return data

    def write(
        self,
        data: DataFrame,
        *largs,
        **kwargs
    ) -> None:

        # Set default read options
        default_write_options: dict = dict(
            format='jdbc',
            user=self.location.username,
            driver="org.postgresql.Driver",
            password=self.location.password,
            forward_spark_s3_credentials=True,
            url=self.jdbc_url,
            mode='default'
        )

        writer = self.build_writer(
            data,
            default_write_options=default_write_options,
            *largs,
            **kwargs
        )

        writer.save()

        return None


class S3Connection(DatabaseConnection):
    """
    Read form and write to S3 buckets.
    """

    def _check_source(self, filter_str, **kwargs):
        """
        Validates read and catches reasons for failure. Proved useful
        when running source existence and has_data.

        Args:
          largs/kwargs passed through to self.read()

        Returns:
          exists, has_data (tuple)
        """

        n_rows = 0
        exists = False

        try:

            logging.info(f'Verifying {self.url} exists.')

            # This will fail of the table / source doesn't exist
            data_frame = (
              self
              .read(
                **kwargs
              )
              .filter(
                sf.expr(filter_str)
              )
            )

            # We only need a single row to confirm existence and data integrity
            # (for now). This can be expanded in time.
            n_rows = len(
              data_frame
              .take(1)
            )

            # If the read works, then the source exists
            exists = True

#         except java.sql.SQLException as e:
#             # Note that this exception handling isn't awesome at the moment.
#             # This will also trigger if there are errors in the SQL statement itself.
#             logging.exception(f'{self.url} does NOT exist.')
#             logging.debug(e)

        except Exception as e:
            
            # Error handling
            logging.info(f'Unexpected Exception. Does your source exist?:\n\n')
            logging.exception(e)

        finally:
            # exists and has_data returned
            has_data = n_rows > 0

            # Print status for debugging
            logging.debug(
                f'{self.url}: Exists: {exists}, Has Data: {has_data}')

        return exists, has_data

    def check_filter(self, filter_str=None, **kwargs):
        """
        Examine the return of connection with specific filter applied.

        Note that small changes are required for S3 (and other non-SQL) reads.
        """

        if filter_str is None:
            filter_str = 'TRUE'

        logging.info(f"Checking filter '{filter_str}'")

#         # Need to do something slightly different for S3 since we can't
#         # (easily) execute SQL directly.
#         data_frame = (
#             self
#             .read(
#                 **kwargs
#             )
#             .filter(
#                 sf.expr(filter_str)
#             )
#         )

        return self._check_source(filter_str)

    def read(
        self,
        spark: Optional[SparkSession] = None,
        **kwargs
    ) -> DataFrame:

        # Set default read options
        default_read_options: dict = dict(
            inferSchema=True,
            header=True,
            format='parquet'
        )

        reader = self.build_reader(
            spark=spark,
            default_read_options=default_read_options,
            **kwargs
        )

        # Finally, load the data and return a pyspark DF
        data = reader.load(self.url)

        return data

    def write(
        self,
        data: DataFrame,
        *largs,
        **kwargs
    ) -> None:

        # Set default options
        # XXX Spark's write API is not homogenous,
        # so we're going to force it to be
        default_write_options = dict(
            mode='default',
            format="parquet"
        )

        writer = self.build_writer(
            data,
            default_write_options=default_write_options,
            *largs,
            **kwargs
        )

        writer.save(self.url)

        return None


class OracleConnection(DatabaseConnection):
    """
    Read from and write to S3 buckets.
    """

    @property
    def jdbc_url(self) -> str:

        location = self.location
        # if(location.port == None):
        #     port = 1521
        # else:
        #     port = location.port

        # XXX `thin` driver hard-coded. Should make this smarter later.
        return f"jdbc:oracle:thin:{location.username}/{location.password}@//{location.hostname}:{location.port}/{location.db}"

    def read(
        self,
        spark: Optional[SparkSession] = None,
        *largs,
        **kwargs
    ) -> DataFrame:

        # Set default read options
        default_read_options = dict(
            user=self.location.username,
            password=self.location.password,
            url=self.jdbc_url,
            # For faster reads
            fetchsize=10000,
            driver="oracle.jdbc.driver.OracleDriver",
            format='jdbc'
        )

        reader = self.build_reader(
            spark=spark,
            default_read_options=default_read_options,
            *largs,
            **kwargs
        )

        # Finally, load the data and return a pyspark DF
        data = reader.load()

        return data

    def check_filter(self, filter_str=None, **kwargs):
        """
        Examine the return of connection with specific filter applied.

        Note that small changes are required for S3 (and other non-SQL) reads.

        Args:
          filter_str (str): filter string to test

        Returns:
          exists, has_data ()
        """

        # Build the query string
        query = f"""
        SELECT
          1
        FROM
          {self.location.schema}.{self.location.table}
        {f'WHERE {filter_str} AND' if filter_str is not None else ''}
        ROWNUM=1"""

        logging.info(f"Checking filter '{filter_str}'")

        # Check the source with a filter in place.
        # We opt to use a SQL query so we push work back to the backend (when possible)
        return self._check_source(query=query)

    def write(
        self,
        data: DataFrame,
        *largs,
        **kwargs
    ) -> None:

        # Set default options
        # XXX Spark's write API is not homogenous,
        # so we're going to force it to be
        default_write_options = dict(
            mode='default',
            # We'll write to CSV by default ... parquet apparently
            # does not fit into the standard write API.
            # Thanks, databricks. Thanks.
            driver="oracle.jdbc.driver.OracleDriver"
        )

        writer = self.build_writer(
            data,
            default_write_options=default_write_options,
            *largs,
            **kwargs
        )

        writer.jdbc(self.jdbc_url, self.location.table)

        return None

# Connections are the workhorse of this library
connection_builder = ConnectionBuilder()
connection_builder.register(('redshift',), RedshiftConnection)
connection_builder.register(('oracle',), OracleConnection)
connection_builder.register(('postgresql',), PostgresqlConnection)
connection_builder.register(('s3a',), S3Connection)

def build_connection(url: str, *largs, **kwargs) -> BaseConnection:
    """
    High-level convenience function to build connection-type objects.

    Args:
      url (str): URL of data or backend
      optional args are passed through to builder

    Returns:
      connection (Connection): returns connection type object
    """

    # Leverage the builder
    connection = connection_builder.build(url, *largs, **kwargs)

    return connection
