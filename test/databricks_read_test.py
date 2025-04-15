# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from data_io import build_connection

# COMMAND ----------

urls_for_test = {
  'redshift://user@rs-cdwdm-tst.c1rrn5bglols.us-east-1.redshift.amazonaws.com/rscdwdm': "lsgds.t_ds_bid_model",
  'redshift://user@gitx-ops-data-warehouse.ccihmrvnkut2.us-east-1.redshift.amazonaws.com:5439/gitx': "lsg_cdp.profile",
  'redshift://user@rs-edsr-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com/edsr': "corpdm.vw_tmo_master",
}

# COMMAND ----------

for url, tbl in urls_for_test.items():
  connector = build_connection(url)
  df = connector.read(
        query=f"""SELECT
                *
              FROM {tbl} LIMIT 1""".strip()
    )
