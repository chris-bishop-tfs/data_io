# Databricks notebook source
import data_io
?data_io.build_connection

# COMMAND ----------

url = 'oracle://user@CDWPRD-rac-db.thermo.com:1521/cdwprd_users'
connection = data_io.build_connection(url)
data = connection.read(query = 'SELECT * FROM CDWREAD.T_PB WHERE ROWNUM < 10')

# COMMAND ----------

# connection.write(data)

# COMMAND ----------



# COMMAND ----------


