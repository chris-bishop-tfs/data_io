# Databricks notebook source
# imports
from data_io.connection import build_connection
from urlpath import URL
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
"""
unittest does note work in databricks (https://stackoverflow.com/questions/67314705/databricks-python-unit-test-error-help-needed),
so we will go with nutter (https://github.com/microsoft/nutter)
"""
from runtime.nutterfixture import NutterFixture, tag

class DataioReadWrite(NutterFixture):
  """
  For this unittest we will kill 2 birds with one stone.
  where going to wirte a dummy dataframe to redshifts, S3, and oracle.
  Than read it back and test it is the same as the dummy dataframe
  """
  
  def __init__(self):
    #our dummy dataframe
    self.df = spark.createDataFrame([
                (1, 'foo'),
                (2, 'bar')],
                ['id', 'label']
                  )
    #s3 test location
    self.s3_url = dbutils.secrets.get(scope='data_io.test', key="s3_test")
    # redshift and oracle location. These database where chosen since I have writing permission 
    # and I will not get introuble for writing to these locations
    self.orcale_url = dbutils.secrets.get(scope='data_io.test', key="oracle_test")
    self.redshift_url = dbutils.secrets.get(scope='data_io.test', key="redshift_test")
    NutterFixture.__init__(self)
    
  #method for testing the dataframes are the same
  def are_dataframes_equal(self, df_actual, df_expected): 
    if df_actual.subtract(df_expected).rdd.isEmpty():
      return df_expected.subtract(df_actual).rdd.isEmpty()
    return False

  #testing s3 read and write
  def assertion_s3_test(self):
    #creating connection
    connection = build_connection(self.s3_url)
    #writing dummie dataframe
    connection.write(self.df, mode='overwrite')
    #now we will test if return the same dataframe
    assert(self.are_dataframes_equal(self.df, connection.read()))
    
  #redshift test read and write
  def assertion_redshift_test(self):
    #creating connection
    connection = build_connection(self.redshift_url)
    #writing dummie dataframe
    connection.write(self.df, mode='overwrite')
    #now we will test if return the same dataframe
    assert(self.are_dataframes_equal(self.df, connection.read()))
    
  #oracle test read and write
  def assertion_oracle_test(self):
    #creating connection
    connection = build_connection(self.orcale_url)
    #writing dummie dataframe
    connection.write(self.df, mode='overwrite')
    #now we will test if the same dataframe is return
    assert(self.are_dataframes_equal(self.df, connection.read()))
    
    
# excute_test from NutterFixture        
result = DataioReadWrite().execute_tests()
print(result.to_string())      

# COMMAND ----------


