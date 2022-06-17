"""Tools and builders used for standardized data IO"""

# Let's setup the building blocks.
import abc
import io
from attr import attrs, attrib
import urllib
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from configparser import RawConfigParser
from urlpath import URL

@attrs
class BaseConnection(abc.ABC):
  """
  Describes a basic connection object. Connections minimally require
  a `read` and `write` method.

  Note: Connections should not be initialized directly. Use the builders
  or higher-level APIs instead.
  """

  url = attrib(default=None)

  @property
  def location(self):
    """
    Store location information as a location object. For now, we'll
    build these on the fly rather than cacheing. But we'll eventually
    want to cache this information (e.g., lru cache)
    """

    return location_builder.build(self.url)

  @property
  def jdbc_url(self):
    """
    JDBC connection strings are required to connect to various backends
    such as Oracle, Redshift, Postgres, etc. This provides a default
    JDBC connection string. Specific connections might require modification
    but this should cover the bulk of LSG's use cases.
    """

    return "jdbc:{scheme}://{hostname}:{port}/{db}".format(
      scheme=self.location.scheme,
      hostname=self.location.hostname,
      port=self.location.port,
      db=self.location.db
    )

  def read(
      self,
      spark=None,
      *largs,
      **kwargs
    ):
    """
    Read data at specificed `location` as a Spark DataFrame

    Args:
      spark (Databricks spark object):
      kwargs: generic read options.
    
    Returns:
      data (Pyspark DataFrame): The data
    """

    raise NotImplemented

  def write(
      self,
      data,
      *largs,
      **kwargs
    ):
    """
    All connections will have a write method, but details
    of write will differ depending on the backend.
    """

    raise NotImplemented
    
  
class BaseBuilder(abc.ABC):
  """
  We'll be constructing several kinds of objects from a URL.
  Rather than bake the build instructions into the objects
  themselves, Bishop opted to leverage a builder pattern
  to handle the instructions required to instantiate the
  underlying objects.

  Attributes:
    handlers (dict): key-value pairs mapping keys to specifi
                     subclasses (e.g., DatabaseLocation)
  
  Methods:
    register: Register a new subclass (handler)
    get_handler: Get handler (value) corresponding to a key
  """

  def __init__(self, *largs, **kwargs):
    super(abc.ABC, self).__init__()
    
    self.handlers = dict()

  def register(self, handler_key, handler):
    """
    Registers a new subclass in handlers dictionary.

    Args:
      handler_key (tuple): dictionary key
      handler (class): subclass
    """

    self.handlers[handler_key] = handler

  def get_handler(self, handler_key):
    """
    Find subclass based on key.

    Args:
      handler_key (tuple):
    
    Returns:
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
  more sense to centralize the functionality
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

    # Generate a location object. This describes where the data are
    location = location_builder.build(url)

    # Let's get the handler so we can do some conditional checks
    connection_class = self.get_handler(url_key)

    # We'll need to expand this check as we add other types of
    # connections that require authentication.
    #
    # At time of writing, the logic here should support JDBC connections
    # like Redshift/Oracle
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

      # Need to get the active spark session
      spark = SparkSession.builder.getOrCreate()

      # Create dbutils object so we can get the user name
      # This enables the package to function outside of a notebook
      dbutils = DBUtils(spark)

      # Reference: https://kb.databricks.com/notebooks/get-notebook-username.html
      # XXX Note: assumes user@ in URL, but this might not be true moving forward
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
    # XXX Hard-coded user@. Sloppy, Bishop. Sloppy.
    cred_base = f"{location.scheme}://user@{location.hostname}/{location.db}"
  
    # Let's get the new credentials.cfg
    # XXX Hard-coded credentials file name. Sloppy, Bishop. Sloppy.
    config_file = self.get_secret('credentials.cfg')

    # Convert to a file-like object
    config_buffer = io.StringIO(config_file)

    # Create a parser so we can strip out the information we need
    # 2021-05-11 CWB: changed config parser to RawConfigParser to
    # bypass interpolation.
    # https://stackoverflow.com/questions/14340366/configparser-and-string-with
    config_parser = RawConfigParser()
    config_parser.read_file(config_buffer)

    # Retrieve username
    # Hard-coding OK here.
    username = config_parser.get(cred_base, 'username')
    password = config_parser.get(cred_base, 'password')

    return username, password


class LocationBuilder(URLKeyBuilder):
  """
  Location Builder generates a location from a fully qualified URL
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


class DatabaseConnection(BaseConnection): 
  """
    Intermediate connection class with common code and default behavior.
  """

  def check_spark_session(self, spark):
    # check for active spark session
    if spark is None:
      spark = SparkSession.builder.getOrCreate()

    return spark
    
  def get_options(self, default_options, is_read, **kwargs):
    # We'll set the default options then override with
    # whatever the user wants
    options = default_options

    for option, value in kwargs.items():
      options[option] = value

    # This is clunky, but we needed a way to support query/dbtable
    if not(isinstance(self, S3Connection)) and is_read and ('query' not in options.keys() and 'dbtable' not in options.keys()):
      options['query'] = f'SELECT * FROM {self.location.schema}.{self.location.table}'
    
    return options
  
  def set_options(self, r_w, options):
    # Set options for the reader object
    for option, value in options.items():
      r_w = r_w.option(option, value)
      
    return r_w
  
  def set_reader(
    self,
    spark=None,
    default_read_options=None,
    *largs,
    **kwargs
  ):
    
    spark = self.check_spark_session(spark)

    read_options = self.get_options(default_read_options, True, **kwargs)

    reader = spark.read.format(read_options['format'])
    reader = self.set_options(reader, read_options)

    return reader
    
  def set_writer(
    self,
    data,
    default_write_options=None,
    *largs,
    **kwargs
  ):

    write_options = self.get_options(default_write_options, False, **kwargs)
    
    writer = data.write

    # set format and mode separately, remove from options dict
    if 'format' in write_options:
      writer = writer.format(write_options['format'])
      write_options.pop('format', None)

    if 'mode' in write_options:
      writer = writer.mode(write_options['mode'])
      write_options.pop('mode', None)

    writer = self.set_options(writer, write_options)

    return writer 
  
  
class RedshiftConnection(DatabaseConnection):
  """
  Redshift connector
  
  XXX Temporary S3 bucket is hard-coded. It will break with other roles.
  XXX Needs hardening.
  """
  
   
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
    
    # Set default read options
    default_read_options = dict(
        user=self.location.username,
        password=self.location.password,
        forward_spark_s3_credentials=True,
        url=self.jdbc_url,
        format="com.databricks.spark.redshift",
        # This is the most questionable to me ... OK to use
        # for multiple people?
        # XXX This should be set through a shared configuration
        # file so other groups can use it
        tempdir='s3a://tfsds-lsg-test/ingestion/redshift_temp',
        # By default, read everything from the table
        # dbtable=f'{self.location.schema}.{self.location.table}'
        # query=f'SELECT * FROM {self.location.schema}.{self.location.table}'
      )

    reader = super().set_reader(
      spark=spark,
      default_read_options=default_read_options,
      *largs,
      **kwargs
    )

    # Finally, load the data and return a pyspark DF
    data = reader.load()
    
    return data

  def write(
    self,
    data,
    *largs,
    **kwargs
  ):

    # Set default options
    # XXX Spark's write API is not homogenous,
    # so we're going to force it to be
    default_write_options = dict(
        format="com.databricks.spark.redshift",
        url=self.jdbc_url,
        user=self.location.username,
        password=self.location.password,
        # 2022-02-15 CB: reenabled default
        dbtable=f"{self.location.schema}.{self.location.table}",
        forward_spark_s3_credentials=True,
        tempdir="s3a://tfsds-lsg-test/ingestion/redshift_temp",
        mode='default'
      )
    
    writer = super().set_writer(
      data,
      default_write_options=default_write_options,
      *largs,
      **kwargs
    )

    writer.save()

    return None
  

class PostgresqlConnection(DatabaseConnection):
  """
  Postgres
  
  This needs to be reorganized so Postgres/Redshift and related connections
  have far, far less redundant code.
  """
  
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
  
    # Set default read options
    default_read_options = dict(
      format='jdbc',
      user=self.location.username,
      driver="org.postgresql.Driver",
      password=self.location.password,
      forward_spark_s3_credentials=True,
      url=self.jdbc_url,
      mode='default'
    )

    reader = super().set_reader(
      spark=spark,
      default_read_options=default_read_options,
      *largs,
      **kwargs
    )

    # Finally, load the data and return a pyspark DF
    data = reader.load()
    
    return data

  def write(
    self,
    data,
    *largs,
    **kwargs
  ):

    # Set default read options
    default_write_options = dict(
      format='jdbc',
      user=self.location.username,
      driver="org.postgresql.Driver",
      password=self.location.password,
      forward_spark_s3_credentials=True,
      url=self.jdbc_url,
      mode='default'
    )

    writer = super().set_writer(
      data,
      default_write_options=default_write_options,
      *largs,
      **kwargs
    )

    writer.save()

    return None
  
  
class S3Connection(DatabaseConnection):
  """
  Read form and write to S3 buckets.
  """
  
  def read(
    self,
    spark=None,
    *largs,
    **kwargs
  ):
    
    # Set default read options
    default_read_options = dict(
        inferSchema=True,
        header=True,
        format='parquet'
      )

    reader = super().set_reader(
      spark=spark,
      default_read_options=default_read_options,
      *largs,
      **kwargs
    )

    # Finally, load the data and return a pyspark DF
    data = reader.load()
    
    return data

  def write(
    self,
    data,
    *largs,
    **kwargs
  ):

    # Set default options
    # XXX Spark's write API is not homogenous,
    # so we're going to force it to be
    default_write_options = dict(
        mode='default',
        format="parquet"
      )

    writer = super().set_writer(
      data,
      default_write_options = default_write_options,
      *largs,
      **kwargs
    )

    writer.save(self.url)

    return None


class OracleConnection(DatabaseConnection):
  """
  Read from and write to S3 buckets.
  """

  @property
  def jdbc_url(self):

    location = self.location

    # XXX `thin` driver hard-coded. Should make this smarter later.
    return f"jdbc:oracle:thin:{location.username}/{location.password}@//{location.hostname}:{location.port}/{location.db}"
  
  def read(
    self,
    spark=None,
    *largs,
    **kwargs
  ):
    # Set default read options
    default_read_options = dict(
        user=self.location.username,
        password=self.location.password,
        url=self.jdbc_url,
        # For faster reads
        fetchsize=10000,
        driver="oracle.jdbc.driver.OracleDriver",
        format='jdbc'
      )

    reader = super().set_reader(
      spark=spark,
      default_read_options=default_read_options,
      *largs,
      **kwargs
    )

    # Finally, load the data and return a pyspark DF
    data = reader.load()
    
    return data

  def write(
    self,
    data,
    *largs,
    **kwargs
  ):

    # Set default options
    # XXX Spark's write API is not homogenous,
    # so we're going to force it to be
    default_write_options = dict(
        mode='default',
        # We'll write to CSV by default ... parquet apparently
        # does not fit into the standard write API.
        # Thanks, databricks. Thanks.
        driver="oracle.jdbc.driver.OracleDriver"
      )

    writer = super().set_writer(
      data,
      default_write_options=default_write_options,
      *largs,
      **kwargs
    )

    writer.jdbc(self.jdbc_url, self.location.table)

    return None 

# Let's configure our builders
location_builder = LocationBuilder()
location_builder.register(('redshift',), DatabaseLocation)
location_builder.register(('oracle',), DatabaseLocation)
location_builder.register(('postgresql',), DatabaseLocation)
location_builder.register(('s3a',), Location)

# Connections are the workhorse of this library
connection_builder = ConnectionBuilder()
connection_builder.register(('redshift',), RedshiftConnection)
connection_builder.register(('oracle',), OracleConnection)
connection_builder.register(('postgresql',), PostgresqlConnection)
connection_builder.register(('s3a',), S3Connection)


def build_connection(url, *largs, **kwargs):
  """
  High-level convenience function to build connection-type objects.
  
  Args:
    url (str): URL of data or backend
    optional args are passed through to builder

  Returns:
    connection (Connection): returns connection type object
  """

  # Leverage the builder
  connection = connection_builder.build(url, *largs, **kwargs)

  return connection
