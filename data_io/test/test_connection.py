from runtime.nutterfixture import NutterFixture, tag
from data_io import build_connection
import io
from configparser import RawConfigParser, ConfigParser
from urlpath import URL
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

''' 
we have now written entire class 
to get the list of our connection in secret
'''

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
    list_cred = []
    secret = self.get_secret('credentials.cfg')
    txt = io.StringIO(secret)
    parser = RawConfigParser()
    config_parser = parser.read_file(txt)
    # getting each url in credential file
    for url in config_parser.sections():
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

"""
unittest for connection will use nutter
"""
class Data_ioConnectioTest(NutterFixture):
  
  def __init__(self):
    # here is where we would ge the connection
    connection = GetConnection()

    self.list_connection =connection.get_connections()
    
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
    check = 'jdbc:' + oracle_list[0].split('user@')[0] + oracle_list[0].split('user@')[1]
    test = build_connection(oracle_list[0]).jdbc_url.split('//')[1]
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


