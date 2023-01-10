# Databricks notebook source
from data_io.connection import build_connection
from urlpath import URL
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
"""
this class is for test mining the credentials
"""

class GetConnection():
  
  def __init__(self):
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    self.scope = (
                dbutils
                .notebook
                .entry_point
                .getDbutils()
                .notebook()
                .getContext()
                .tags()
                .apply('user')
                # XXX Hack incoming
                .split('@')
                [0]
            )
  # get secret
  def get_secret(self, key):
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    return dbutils.secrets.get(scope=self.scope, key=key)
    
  # providing list of creds
  def get_connections(self):
    list_creds = []
    secret = self.get_secret('credentials.cfg')
    connection_list = []
    for st in secret.split('\n'):
      # checking if this is connection
      try:
        if('[' == st[0]):
          # appending connection
          connection_list.append(st.split('[')[1].split(']')[0])
      except:
       # string has no lengh
       print('no index')
    # getting each url in credential file
    list_cred = []
    for url in connection_list:
      path = URL(url)
      # adding port
      if((path.port == None) and (path.scheme != 's3a')):
        # orracle
        if(path.scheme == 'oracle'):
          url_list = url.split('.com/')
          url = url_list[0] + '.com' + ':1521' + '/' + url_list[1]
        # redshift
        elif(path.scheme == 'redshift'):
          url_list = url.split('.com/')
          url = url_list[0] + '.com' + ':5439' + '/' + url_list[1]
      #appending list of url
      list_cred.append(url)
      
    return list_cred

# COMMAND ----------

"""
unittest does note work in databricks (https://stackoverflow.com/questions/67314705/databricks-python-unit-test-error-help-needed),
so we will go with nutter (https://github.com/microsoft/nutter)
"""

from runtime.nutterfixture import NutterFixture, tag

class DataioConnectioTest(NutterFixture):
  
  def __init__(self):
    # here is where we would ge the connection
    object_ = GetConnection()
    self.list_connection = object_.get_connections()
    
    NutterFixture.__init__(self)
    
  # getting redshift url
  def get_redshift_url(self):
    redshift_list=[]
    for url in self.list_connection:
      
      if(url.split('://')[0] == 'redshift'):
        redshift_list.append(url)
        
    #now we will set up test and connection
    check = 'jdbc:' + redshift_list[0].split('user@')[0] + redshift_list[0].split('user@')[1]
    test = build_connection(redshift_list[0]).jdbc_url
#     print(test)
#     print(check)
    return test, check

  # getting oraacle url
  def get_oracle_url(self):
    oracle_list=[]
    for url in self.list_connection:
      
      if(url.split('://')[0] == 'oracle'):
        oracle_list.append(url)
        
    check = oracle_list[0].split('@user')[1]
    test = build_connection(oracle_list[0])
        
    #now we will set up test and connection
    check = 'jdbc:' + redshift_list[0].split('user@')[0] + redshift_list[0].split('user@')[1]
    test = build_connection(redshift_list[0]).jdbc_url.split('//')[1]
#     print(test)
#     print(check)
    return test, check
    
  #testing connection to redshift
  def assertion_redshift_test(self):
    # getting varraibles
    test, check = self.get_redshift_url()

    assert(test == check)
    
  #testing oracle connection
  def assertion_oracle_test(self):
    
    test, check = self.get_redshift_url()

    assert(test == check)
        
# excute_test from NutterFixture        
result = DataioConnectioTest().execute_tests()
print(result.to_string())       

# COMMAND ----------


