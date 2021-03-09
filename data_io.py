# Databricks notebook source
# MAGIC %md
# MAGIC ## Overview
# MAGIC 
# MAGIC Bishop has been looking through projects, connectors, and code and thinks there is value to standardizing the API for data retrieval. Bishop has been exploring Pyspark Optimus as an option, but he is encountering what appear to be driver mismatches, etc. Bishop thinks Optimus would be an excellent option, but is currently taking too much time to debug. Instead opting to get an API in place and will incorporate Optimus later; the implementation specifics will be hidden behind the API anyway.

# COMMAND ----------

# Let's setup the building blocks.
import abc
from attr import attrs, attrib
import urllib
from pyspark.sql import SparkSession

from urlpath import URL

@attrs
class BaseConnection(abc.ABC):
  """
  Base connection object.
  """

  url = attrib(default=None)

  @property
  def location(self):
    """
    We'll dynamically build the location object in
    case we change information at some point.
    
    This can probably just be cached safely.
    """

    return location_builder.build(self.url)

  @property
  def jdbc_url(self):
    """
    JDBC connection string. Might need to be modified in the builder
    """

    return "jdbc:{scheme}://{hostname}:{port}/{db}".format(
      scheme=self.location.scheme,
      hostname=self.location.hostname,
      port=self.location.port,
      db=self.location.db
    )

  def read(self, spark=None, *largs, **kwargs):
    """
    All connections will have a `read` method, but the details
    of the read will differ depending on the backend.
    """

    raise NotImplemented

  def write(self, data, *largs, **kwargs):
    """
    All connections will have a write method, but details
    of write will differ depending on the backend.
    """
    raise NotImplemented


class BaseBuilder(abc.ABC):
  """
  We'll be constructing several kinds of objects from a URL.
  Rather than bake the build instructions into the objects
  themselves, Bishop opted to leverage a builder to handle
  the instructions required to instantiate the underlying
  classes.
  """

  def __init__(self, *largs, **kwargs):
    super(abc.ABC, self).__init__()
    
    self.handlers = dict()

  def register(self, handler_key, handler):
    """
    Register a new handler
    """

    self.handlers[handler_key] = handler

  def get_handler(self, handler_key):
    """
    Retrieve the required handler
    """

    return self.handlers[handler_key]

  @abc.abstractmethod
  def build(self):
    """
    This is the custom piece of a builder.
    
    It will returned a constructed object
    """

    raise NotImplemented


class URLKeyBuilder(BaseBuilder):
  """
  Simple extension of a builder that adds a method
  to build a handler key from a URL.
  
  Bishop ended up needing to build identical keys for
  both Location and Connection builders. So, it made
  made more sense to centralize the functionality
  """

  def __init__(self, *largs, **kwargs):
    super(BaseBuilder, self).__init__()

  def build_url_key(self, url):
    """
    Parse URL to create the required key
    """

    # Convert URL into a location object
    # This will be parsed, etc. correctly
    location = URL(url)

    # Build the connection key from the URL
    #  scheme (protocol), extension?
    #
    #  KISS and use protocol only to start
    url_key = (location.scheme,)

    return url_key


class ConnectionBuilder(URLKeyBuilder):
  """
  Builds the appropriate connection object from a URL.

  Adding Connections:
    - Write connection class
    - Register with builder
  """

  def __init__(self, *largs, **kwargs):
    super(URLKeyBuilder, self).__init__()

  def build(self, url):
    """
    Build a connection and return it
    """

    # Get the key
    url_key = self.build_url_key(url)

    # Add logic to lookup credentials etc.
    # XXX
    location = location_builder.build(url)

    # Let's get the handler so we can do some
    # conditional checks
    connection_class = self.get_handler(url_key)

    # We'll need to expand this check as we add other types of
    # connections that require authentication.
    if isinstance(location, DatabaseLocation):
      """
      Grab connection information from stored config file.
      """
    
      # Get credentials
      username, password = self.get_credentials(url)

      # Alter location parts to create a new URL
      # Ran into URL parsing issues, so needed to
      # quote username and password
      _url = (
        url
        .replace(
          'user@',
          f'{urllib.parse.quote(username)}:{urllib.parse.quote(password)}@'
        )
      )

    else:
      
      _url = url

    # Need a little more information (potentially)
    # for Redshift connections (e.g., s3 bucket)
    if isinstance(connection_class, RedshiftConnection):
      
      pass

    connection = connection_class(_url)

    return connection
  
  def get_secret(self, key, scope=None):
    """
    Retrieve a desired key from the specified secrets scope
    """
    
    if scope is None:
      # XXX This is almost certainly going to break
      # when we move this code out of a notebook.
      # Reference: https://kb.databricks.com/notebooks/get-notebook-username.html
      scope = (
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

    # Retrieve the secret, yo!
    secret = (
      dbutils
      .secrets
      .get(
        scope=scope,
        key=key
      )
    )
  
    return secret
  
  def get_credentials(self, url):
    """
    Needed a way to do routine credential lookups.
    
    At time of writing, credentials will be stored as
    databricks secrets registered in a user-specific
    scope. While credentials are often shared
    (e.g., service accounts) this allows us the flexibility
    of using different accounts to improve security.
    
    Future dev:
    - Default lookup to DSLSG_SCOPE?
    """
    
    # Figure out the relevant base string for the
    # username and credentials.
    location = location_builder.build(url)
  
    # Makes formatted strings below nicer to work with
    cred_base = f"{location.scheme}://user@{location.hostname}/{location.db}"
  
    # Hard coding here is a bit sloppy, but fine for MVP
    username_key = cred_base + '-username'
    password_key = cred_base + '-password'

    username = self.get_secret(username_key)
    password = self.get_secret(password_key)

    return username, password


class LocationBuilder(URLKeyBuilder):
  """
  Ingest a URL and return the correct kind of location
  object
  """

  def __init__(self, *largs, **kwargs):
    super(URLKeyBuilder, self).__init__()

  def build(self, url):
    
    # Build the URL key
    url_key = self.build_url_key(url)
    
    location_class = self.get_handler(url_key)

    # Get the location object
    location = location_class(url)
    
    return location


class Location(URL):
  """
  Store URL information, will likely need to extend this
  to do some custom things later. At the moment, just a
  stub for extension
  """

  def __init__(self, *largs, **kwargs):
    super(URL, self).__init__()

  pass


class DatabaseLocation(Location):
  """
  Database connections require extra parsing
  """

  def __init__(self, *largs, **kwargs):
    super(Location, self).__init__()
    
    # Additional parsing and attributes for
    # databases
    # XXX
    path_split = self.path.replace('/', '').split('.')
    
    # Need some conditional formatting here
    if len(path_split) == 3:
      # db, schema, table
      self.db, self.schema, self.table = path_split
  
    elif len(path_split) == 2:
      # db, table
      # Occurs with backends like Impala
      self.db, self.table = path_split
      self.schema = None
    
    # XXX Add another use case with just the database
    elif len(path_split) == 1:
      self.db = path_split[0]
      self.schema = self.table = None
    else:
      raise NotImplemented

# # Other stubs
# class S3Location(Location):
  
#   pass

# class FileLocation(Location):
  
#   pass
class RedshiftConnection(BaseConnection):
  """
  Redshift connector
  
  XXX How do we specify underlying S3 bucket?
  XXX Do we need to specify it?
  XXX If so, can we detect the underlying bucket
  XXX automatically> No idea. need to deal with this
  """

  def write(self):
    
    pass

  def read(
      self,
      spark=None,
      *largs,
      **kwargs
    ):
    """
    Read method for redshift data sources.
    
    Named inputs are assumed to be read parameters
    
    Args:
      spark (spark session): spark thingy
    
    Returns:
      data (spark DF): the data, YO
    """

    if spark is None:
      
      spark = SparkSession.builder.getOrCreate()
  
    # Set default read options
    default_read_options = dict(
        user=self.location.username,
        password=self.location.password,
        forward_spark_s3_credentials=True,
        url=self.jdbc_url,
        # This is the most questionable to me ... OK to use
        # for multiple people?
        # XXX This should be set through a shared configuration
        # file so other groups can use it
        tempdir='s3a://tfsds-lsg-test/ingestion/redshift_temp',
        # By default, read everything from the table
        query=f'SELECT * FROM {self.location.schema}.{self.location.table}'
      )

    # We'll set the default read options then override with
    # whatever the user wants
    read_options = default_read_options
  
    for option, value in kwargs.items():

      read_options[option] = value
  
    # Initialize the reader
    reader = (
      spark
      .read
      .format("com.databricks.spark.redshift")
    )
    
    # Set options for the reader object
    for option, value in read_options.items():
      
      reader = reader.option(option, value)

    # Finally, load the data and return a pyspark DF
    data = reader.load()

    return data


class S3Connection(BaseConnection):
  """
  Read from and write to S3 buckets.
  """
  
  def read(
    self,
    spark=None,
    *largs,
    **kwargs
  ):

    if spark is None:
      
      spark = SparkSession.builder.getOrCreate()
  
    # Set default read options
    default_read_options = dict(
      inferSchema=True,
      header=True
      # Need a nicer query to run against S3
#         query=f'SELECT * FROM {self.location.schema}.{self.location.table}'
      )

    # We'll set the default read options then override with
    # whatever the user wants
    read_options = default_read_options
  
    for option, value in kwargs.items():

      read_options[option] = value
  
    # Initialize the reader
    # Set reader format
    reader = (
      spark
      .read
    )

    # Set options for the reader object
    for option, value in read_options.items():
      
      reader = reader.option(option, value)
    
    data = reader.load(self.url)
    
    return data


class OracleConnection(BaseConnection):
  """
  Read from and write to S3 buckets.
  """
  
  @property
  def jdbc_url(self):
    
    return f"jdbc:oracle:thin:{username}/{password}@//{hostname}:{portnumber}/{service_name}"

  def read(
    self,
    spark=None,
    *largs,
    **kwargs
  ):
  # XXX I'm copying and pasting code, need better abstraction

    if spark is None:
      
      spark = SparkSession.builder.getOrCreate()

    # Set default read options
    default_read_options = dict(
        user=self.location.username,
        password=self.location.password,
        url=self.jdbc_url,
        # By default, read everything from the table
        query=f'SELECT * FROM {self.location.schema}.{self.location.table}',
        driver="oracle.jdbc.driver.OracleDriver",
        # XXX Should we add in defaults for lowerbound, upper...
      )

    # We'll set the default read options then override with
    # whatever the user wants
    read_options = default_read_options

    reader = spark.read.format('jdbc')

    for option, value in kwargs.items():

      read_options[option] = value

    # Set options for the reader object
    for option, value in read_options.items():

      reader = reader.option(option, value)

    # Finally, load the data and return a pyspark DF
    data = reader.load()
  
    return data

# Let's configure our builders
location_builder = LocationBuilder()
location_builder.register(('redshift',), DatabaseLocation)
location_builder.register(('oracle',), DatabaseLocation)
location_builder.register(('s3a',), Location)

# Connections are the workhorse of this library
connection_builder = ConnectionBuilder()
connection_builder.register(('redshift',), RedshiftConnection)
connection_builder.register(('oracle',), OracleConnection)
connection_builder.register(('s3a',), S3Connection)


def build_connection(url, *largs, **kwargs):
  """
  High-level convenience function to build connection-type objects
  
  Args:
    url (str): URL of data or backend
  
  Returns:
    connection (Connection): returns connection type object
  """
  
  # Leverage the builder
  connection = connection_builder.build(url, *largs, **kwargs)

  return connection

# COMMAND ----------

# Where are the data?
url = 'redshift://user@rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com:5439/rscdwdm.lsgds.sf_db_data__c'

# Test connectivity at database level
url = 'redshift://user@rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com:5439/rscdwdm'
# Create a connection using the API wrapper
connection = build_connection(url)

hostname = 'CDWPRD-rac-db.thermo.com'
port: 1521
portnumber = 1521
service_name = 'cdwprd_users'
schema = 'CDWREAD'

# Let's try oracle
url = f'oracle://user@{hostname}:{portnumber}/{service_name}'
  
# # connection = build_connection(url)
# print(url)

# # Let's do it
connection = build_connection(url)

data = connection.read(query='SELECT * FROM CDWREAD.T_PB')

# connection_builder.build(url)

# COMMAND ----------

url = 's3a://tfsds-lsg-test/model-output-archive/backup/DrugDiscovery_txns'

connection = build_connection(url)

# data = (
#   spark
#   .read
#   .format('parquet')
#   .option('inferSchema', True)
#   .option('url', 's3a://tfsds-lsg-test/model-output-archive/backup/DrugDiscovery_txns')
#   .load()
# )
# data = spark.read.parquet('s3a://tfsds-lsg-test/model-output-archive/backup/DrugDiscovery_txns')

# data.printSchema()
# connection = build_connection(url)
data = connection.read()

data.printSchema()

# COMMAND ----------

# location = connection.location
# "jdbc:{scheme}://{hostname}:{port}/{db}".format(
#       scheme=location.scheme,
#       hostname=location.hostname,
#       port=location.port,
#       db=location.db
#     )

# connection = connection_builder.build(url)

# location_builder.build(connection.url)
# connection.url
# x = urlsplit(connection.url)
# 
# x.port
# x = location_builder.build('redshift://Lsgds_Read:San#Jun202009_Pwd@rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com:5439/rscdwdm.lsgds.sf_db_data__c')

connection = connection_builder.build(url)

# data = connection.read(read_options=dict(query="SELECT account_key__c FROM lsgds.sf_db_data__c LIMIT 10"))
# x = urlsplit(f'redshift://Lsgds_Read:San%23Jun202009_Pwd@rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com:5439/rscdwdm.lsgds.sf_db_data__c')

# x.port

# COMMAND ----------

data = connection.read(query='SELECT * FROM CDWREAD.T_PB')

# COMMAND ----------

import cx_Oracle
import pandas as pd

def read_edw(table_query,cache):
  query = table_query

  try:
      cdw_con = cx_Oracle.connect(dbutils.secrets.get(scope="DSLSG_Scope",key="ORACLE_EDW_PRD_BM_Username"), dbutils.secrets.get(scope="DSLSG_Scope",key="ORACLE_EDW_PRD_BM_Password"), "edwsprd-rac-db.thermo.com/EDWPRD_USERS")
  except Exception as e:
      print(e)
      print('Connection failed')
  cdw_cur = cdw_con.cursor()
  cdw_cur.execute(query)
  result = cdw_cur.fetchall()
  data = pd.DataFrame(result)
  data.columns = [i[0] for i in cdw_cur.description]
  out = spark.createDataFrame(data)
  
  if cache == True:
    out.createOrReplaceTempView('output')
    spark.sql('CACHE TABLE output')
  
  return out

# Let's get some credentials
username = 'CDWREAD'# dbutils.secrets.get(scope="DSLSG_Scope",key="ORACLE_EDW_PRD_BM_Username")
password = 'CDWREAD1234' #dbutils.secrets.get(scope="DSLSG_Scope",key="ORACLE_EDW_PRD_BM_Password")
hostname = 'CDWPRD-rac-db.thermo.com'
port: 1521
portnumber = 1521
service_name = 'cdwprd_users'
schema = 'CDWREAD'


# [5:11 PM] Mohan, Bharath
#     def conn_config(host,port,db,username,password):
#   jdbcUrl = "jdbc:oracle:thin:@//{​​​​​​​0}​​​​​​​:{​​​​​​​1}​​​​​​​/{​​​​​​​2}​​​​​​​".format(host, port, db)
#   connectionProperties = {​​​​​​​
#     "user" : username,
#     "password" : password,
#     "driver" : "oracle.jdbc.driver.OracleDriver",
#     "oracle.jdbc.timezoneAsRegion" : "false" }​​​​​​​
#   return (jdbcUrl,connectionProperties)

jdbc_url = f"jdbc:oracle:thin:{username}/{password}@//{hostname}:{portnumber}/{service_name}"

data = (
  spark.read
  .format("jdbc")
  .option("url", jdbc_url)
  .option("query", 'SELECT * FROM CDWREAD.T_PB')
  .option("user", username)
  .option("password", password)
  .option("driver", "oracle.jdbc.driver.OracleDriver")
  .load()
)

# COMMAND ----------

# data.show()

# COMMAND ----------

import configparser
import json

cfgparser = configparser.ConfigParser()

credentials = connection_builder.get_secret('credentials.ini').title()[1:-1]

print(credentials)

# COMMAND ----------

# import urllib
# from urllib import
import urllib
x = urlsplit(urllib.parse.quote(connection.url))
x
# urllib.quote(connection.url)

# COMMAND ----------

x = dbutils.secrets.get('chris.bishop', 'redshift://user@rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com/rscdwdm-password')

# COMMAND ----------

str(x.format()

# COMMAND ----------

x = location_builder.build(url)
# x = BaseConnection(url)
# x = connection_builder.build(url)
x = DatabaseLocation(url)
x.port

# COMMAND ----------

connection_builder.build(url)
connection = connection_builder.build(url)
# connection
location_key = location_builder.build_url_key(url)

location_class = location_builder.get_handler(location_key)

location_class(url)
# location = location_builder.build('redshift://Lsgds_Read:San#Jun202009_Pwd@rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com:5439/rscdwdm.lsgds.sf_db_data__c')

# COMMAND ----------

location.password

# COMMAND ----------

location = connection.location

(
  spark.read
  .format("com.databricks.spark.redshift")
  .option("url", connection.jdbc_url)
  .option("user", location.username)
  .option("password", location.password)
  .option("query", "SELECT * FROM lsgds.sf_db_data__c LIMIT 10")
  .option("tempdir", 'whatever')
  .option("forward_spark_s3_credentials", True)
  .load()
)

# COMMAND ----------

# username, password = connection_builder.get_credentials(url)
location = location_builder.build(url)
# str(username.title())
# username.title()
# x = connection_builder.get_secret('redshift://user@host.com/db-password')
# location.hostname, location.port

# f'{location.schema}.{location.table}'

# COMMAND ----------

def rs_conn_config(host,port,db):
  RS_jdbcUrl = "jdbc:redshift://{0}:{1}/{2}".format(host, port, db)
  return (RS_jdbcUrl)

x = rs_conn_config(location.hostname, location.port, location.db)
connection.jdbc_url, x

# COMMAND ----------

connection = connection_builder.build(url)

# reader = connection.read()
# Let's try to read
# # connection.location
read_options = connection.read(read_options=dict(query="SELECT * FROM lsgds.sf_db_data__c LIMIT 10"))
# # connection_builder.get_credentials(url)
# # connection_builder.build(url)
# # location = connection.location
# location = location_builder.build(url)
# default_read_options = dict(
#         user=location.username,
#         password=location.password,
#         forward_spark_s3_credentials=True,
#         url=self.jdbc_url,
#         # This is the most questionable to me ... OK to use
#         # for multiple people? XXX
#         tempdir='s3a://tfsds-lsg-test/ingestion/redshift_temp',
#         query=f'SELECT * FROM {self.location.schema}.{self.location.table}'
#       )
# # p = dbutils.secrets.get(scope="DSLSG_JDBC_Scope",key="RS_CDWDS_TST_COE_Password")
# # location = location_builder.build(url)
# # connection = connection_builder.build(url)
# # # # builder._build_key(url)
# # # tuple(['redshift']) is builder._bui
# # builder.handlers[builder._build_key(url)]
# # connection = builder.build(url)

# # location
# # connection.location

# COMMAND ----------

location_builder.build(connection.url)

# COMMAND ----------

from pyspark.sql import SparkSession
x = SparkSession.builder.getOrCreate()

x.read

# COMMAND ----------

spark.getActiveSession()

# COMMAND ----------

x.getActiveSession()

# COMMAND ----------

def update_url(url, **kwargs):
  
  # Parse the URl into pieces
  parse_result = urlparse(url)

  # Create key-value pairs from parsed URL
  parse_params = dict(
    scheme=parse_result.scheme,
    netloc=parse_result.hostname,
    path=parse_result.path,
    fragment=parse_result.fragment,
    params=parse_result.params,
    query=parse_result.query
  )
  
  # Override things
  for k, v in kwargs.items():
    parse_params[k] = v
  
  result_update = ParseResult(**parse_params)
  
  return result_update.geturl()

# update_url(url, username='whatever')
# u = urlsplit(url)

x = URL(url)
# # Create key-value pairs from parsed URL
# parse_params = dict(
#   scheme=parse_result.scheme,
#   netloc=parse_result.hostname,
#   path=parse_result.path,
#   fragment=parse_result.fragment,
#   params=parse_result.params,
#   query=parse_result.query
# )


# # And override
# parse_result.geturl()
# ParseResult(scheme=u.scheme, netloc=u.hostname, path=u.path, fragment=u.fragment, params=None, query=None)
# # parse_result.username = 'asdflaksjdf;laskjdf'

# COMMAND ----------

parse_result.netloc

# COMMAND ----------

from optimus import Optimus

# op= Optimus(
#   repositories="myrepo",
#   packages="com.databricks.spark.redshift",
#   jars="my.jar",
# #   driver_class_path="dbfs:/FileStore/jars/3245c868_1a14_4a2d_9b6c_48db9f40bd64-RedshiftJDBC42_no_awssdk_1_2_47_1071-1ed67.jar",
# #   driver_class_path='/databricks/python/lib/python3.7/site-packages/optimus/jars/RedshiftJDBC42-1.2.16.1027.jar'
#   verbose=True
# )

op = Optimus(spark)

# COMMAND ----------

op = Optimus(spark)

# COMMAND ----------

# import dbutils
# #get data from redshift prod
def rs_conn_config(host,port,db):
  RS_jdbcUrl = "jdbc:redshift://{0}:{1}/{2}".format(host, port, db)
  return (RS_jdbcUrl)

# def read_redshift(table_query,env,cache):
  
#   if env == 'TST': 
#     RS_host = "cdwtst.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com"
#     RS_port = '5439'
#     RS_db = 'cdwtst'
#     RS_username = 'cdwds_user'
#     RS_password =  dbutils.secrets.get(scope="DSLSG_JDBC_Scope",key="RS_CDWDS_TST_COE_Password")
#     AWS_BUCKET_NAME = 's3a://tfsds-lsg-test/ingestion/redshift_temp'
#     RS_jdbcUrl = rs_conn_config(RS_host, RS_port, RS_db)
#   else:
#     RS_host = "rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com"
#     RS_port = '5439'
#     RS_db = 'rscdwdm'
#     RS_username = 'cdwds_user'
#     RS_password =  dbutils.secrets.get(scope="DSLSG_JDBC_Scope",key="RS_CDWDS_PRD_COE_Password")
#     AWS_BUCKET_NAME = 's3a://tfsds-lsg-test/ingestion/redshift_temp'
#     RS_jdbcUrl = rs_conn_config(RS_host, RS_port, RS_db)
#   query = table_query
#   out = spark.read \
#   .format("com.databricks.spark.redshift") \
#   .option("url", RS_jdbcUrl) \
#   .option("user",RS_username) \
#   .option("password",RS_password) \
#   .option("query", query) \
#   .option("tempdir", AWS_BUCKET_NAME) \
#   .option("forward_spark_s3_credentials", True) \
#   .load()
  
#   if cache == True:
#     out.createOrReplaceTempView('output')
#     spark.sql('CACHE TABLE output')
  
#   return out

# COMMAND ----------

import abc
from attr import attrs, attrib

@attrs
class Location(object):
  
  url = attrib()
  schema = attrib()
  
  

# COMMAND ----------

# Let's get Optimus to work with our Redshift instance.
# Examples from here https://espressofx.medium.com/data-cleansing-on-pyspark-using-optimus-b46c344804f5
from optimus import Optimus

# This import is only to hide the credentials
op = Optimus(spark)

# COMMAND ----------

# Get credentials
RS_host = "rs-cdwdm-prd.c7cbvhc6rtn1.us-east-1.redshift.amazonaws.com"
RS_port = '5439'
RS_db = 'rscdwdm'
RS_username = 'cdwds_user'
RS_password =  dbutils.secrets.get(scope="DSLSG_JDBC_Scope",key="RS_CDWDS_PRD_COE_Password")
AWS_BUCKET_NAME = 's3a://tfsds-lsg-test/ingestion/redshift_temp'
RS_jdbcUrl = rs_conn_config(RS_host, RS_port, RS_db)

# COMMAND ----------

RS_jdbcUrl

# COMMAND ----------

out = (
  spark.read
  .format("com.databricks.spark.redshift")
  .option("url", RS_jdbcUrl)
  .option("user",RS_username)
  .option("password",RS_password)
  .option("query", "SELECT * FROM lsgds.sf_db_data__c LIMIT 10")
  .option("tempdir", AWS_BUCKET_NAME)
  .option("forward_spark_s3_credentials", True)
#   .option('schema', 'lsgds')
#   .option('dbtable', 'sf_db_data__c')
  .load()
)

# COMMAND ----------

out.show()

# COMMAND ----------

op.connect

# COMMAND ----------



# COMMAND ----------

data_base = op.connect(
    driver='redshift',
    host=RS_host,
    database=RS_db,
    user=RS_username,
    password=RS_password,
    port=RS_port,
    schema='lsgds'
#     table_name='sf_db_data__c'
)

data_base.driver_properties.value['java_class'] = 'com.databricks.spark.redshift'

# COMMAND ----------



# COMMAND ----------

data_base.tables('lsgds')

# COMMAND ----------

out = (
  # Test the configured spark context in Optimus
  op
  .spark
  .read
  .format("com.databricks.spark.redshift")
  .option("url", RS_jdbcUrl)
  .option("user",RS_username)
  .option("password",RS_password)
  .option("query", "SELECT * FROM lsgds.sf_db_data__c LIMIT 10")
  .option("tempdir", AWS_BUCKET_NAME)
  .option("forward_spark_s3_credentials", True)
  .option('schema', 'lsgds')
#   .option('dbtable', 'sf_db_data__c')
  .load()
)

# COMMAND ----------

(
  data_base
  .driver_properties
  .list()
)

# COMMAND ----------

data_base.driver_context.properties()

# COMMAND ----------

redshift

# COMMAND ----------

redshift
