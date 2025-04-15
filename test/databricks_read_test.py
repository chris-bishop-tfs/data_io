# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from data_io import build_connection
from urllib.parse import urlparse

# COMMAND ----------

urls_for_test = [
  'redshift://user@rs-cdwdm-tst.c1rrn5bglols.us-east-1.redshift.amazonaws.com/rscdwdm.lsgds.t_ds_bid_model',
  'redshift://user@rs-cdwdm-tst.c1rrn5bglols.us-east-1.redshift.amazonaws.com/rscdwdm.lsgmo.lsg_winback',
  'redshift://user@gitx-ops-data-warehouse.ccihmrvnkut2.us-east-1.redshift.amazonaws.com:5439/gitx.lsg_cdp.profile',
  'redshift://user@rs-edsr-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com/edsr.corpdm.vw_tmo_master',
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
