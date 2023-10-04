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
      # creating empty dataframe for test
      column_names = "ColA|ColB|ColC"
      self.df_empty  = spark.createDataFrame(
                  [
                   tuple('' for i in column_names.split("|"))
                ],
                column_names.split("|")
                ).where("1=0")
      self.url_empty = 'redshift://user@gitx-ops-data-warehouse.ccihmrvnkut2.us-east-1.redshift.amazonaws.com/gitx.dev_ds.test_no_data'
      build_connection(self.url_empty).write(self.df_empty, mode='overwrite')
      # wrong date dataframe
      self.df_not_append = spark.createDataFrame([
                (1, datetime.date.today() + datetime.timedelta(days=10), 'foo'),
                (2, datetime.date.today() + datetime.timedelta(days=10), 'bar')],
                ['id', 'time_prd_val', 'label']
                  )
      # url of date
      self.url_worng_date = 's3a://tfsds-lsg-test/test_worng_date'
      build_connection(self.url_worng_date).write(self.df_not_append, mode='overwrite')
      # url non existing dataframe
      self.url_non_exsit = 's3a://tfsds-lsg-test/test_not_exsit'
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

  # checking false return for data
  def assertion_has_data_false(self):

    assert(not build_data_source(self.url_empty, None).has_data())

  # checkin does not exsit
  def assertion_not_exsit(self):

    assert(not build_data_source(self.url_non_exsit, None).exists())

  # checking has not been appended
  def assertion_not_append(self):

    assert(not build_data_source(self.url_worng_date, None).has_been_append(self.date_column))


#run test
result = DataSourceTest().execute_tests()
print(result.to_string())

    


