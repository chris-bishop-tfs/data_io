# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from data_io import build_connection
from urllib.parse import urlparse

# COMMAND ----------

urls_for_test = [
]

# COMMAND ----------

# compare password parsing
for url in urls_for_test:
  print(url)
  connector = build_connection(url=url)
  print(connector.location.username)
  df = connector.read()

# COMMAND ----------

# compare password parsing
for url, tbl in urls_for_test.items():
  connector = build_connection(url=url, db_schema=tbl.split('.')[0])
  lib_parse = urlparse(url)
  assert lib_parse.password == connector.location.password, f"Expected {connector.location.password} but got {lib_parse.password}"
