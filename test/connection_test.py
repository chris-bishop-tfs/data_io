# Databricks notebook source
test = build_connection('redshift://user@gitx-ops-data-warehouse.ccihmrvnkut2.us-east-1.redshift.amazonaws.com/gitx')
test.jdbc_url

# COMMAND ----------

# from data_io.connection import build_connection
from data_io.get_credentials import GetConnection


# COMMAND ----------


