# Databricks notebook source
from data_io import build_connection
import pyspark.sql.functions as sf

web_df = build_connection('s3a://tfsdl-adobe-prod/thermofisher.com/processed/tfs_hit_data/').read(format = 'delta').filter(sf.col('hit_time_gmt_dt_key') >= 20211001).cache()


# COMMAND ----------

display(web_df)

# COMMAND ----------


