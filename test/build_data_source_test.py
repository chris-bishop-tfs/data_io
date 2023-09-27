# Databricks notebook source
# imports
from data_io.connection import build_connection
from data_io.data_source import build_data_source
import datetime
from urlpath import URL
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
"""
unittest does note work in databricks (https://stackoverflow.com/questions/67314705/databricks-python-unit-test-error-help-needed),
so we will go with nutter (https://github.com/microsoft/nutter)
"""
from runtime.nutterfixture import NutterFixture, tag

class DataSourceTest(NutterFixture):

  """
    This unittest will test build_data_source
    exsit(), has_data(), and has_been_append() 
    method.
   """
  def __init__(self):
        #our dummy dataframe
      self.df = spark.createDataFrame([
                (1, datetime.date.today(), 'foo'),
                (2, datetime.date.today(), 'bar')],
                ['id', 'time_prd_val', 'label']
                  )
        # column for append method
      self.date_column = 'time_prd_val'
        #url store dataframe
      self.url ='s3a://tfsds-lsg-test/test'
        #saving dataframe for the check
      build_connection(self.url).write(self.df, mode='overwrite')
      NutterFixture.__init__(self)

  # has data check
  def assertion_has_data(self):
     
    assert(build_data_source(self.url, None).has_data())

  # checking if exsit
  def assertion_exsit(self):
     
    assert(build_data_source(self.url, None).exists())

  # checking has been appended
  def assertion_has_been_appended(self):
     
    assert(build_data_source(self.url, None).has_been_append(self.date_column))

  # removing test dataframe
  def assertion_dataframe_remove(self):

    dbutils.fs.rm(self.url, True)
    assert(not build_data_source(self.url, None).exists())

#run test
result = DataSourceTest().execute_tests()
print(result.to_string())

    


