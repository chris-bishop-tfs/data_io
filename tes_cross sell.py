# Databricks notebook source
from data_io import build_connection
url = 'oracle://user@x03s-scan.amer.thermo.com:1521/CDWTST_USERS.cross_sell'
connection = build_connection(url)
df = connection.read()
display(df)

# COMMAND ----------


