"""
Data source classes extend connections with additional utility and checks.
These proved useful as a set of base classes around which automated checks
are built.
"""
from .builder import BaseBuilder
from .connection import build_connection, BaseConnection
import abc
import logging
from attr import define, field
import datetime
import pyspark.sql.functions as sf

@define
class DataSource(abc.ABC):
  
  # We'll use this for data checks
  connection: BaseConnection
  filter_str: str=field(init=True, default=None)

  def exists(self, *largs, **kwargs):
    
    # Run existence and has_data checks
    exists, _ = (
      self
      .connection
      .check_filter(
        *largs,
        filter_str=self.filter_str,
        **kwargs
      )
    )

    return exists

  def has_data(self, *largs, **kwargs):
    """
    
    """

    # Run existence and has_data checks
    _, has_data = (
      self
      .connection
      .check_filter(
        *largs,
        filter_str=self.filter_str,
        **kwargs
      )
    )

    return has_data
  
  def has_been_append(self, time_collumn : str):
    """
    Checks if a table has been appended for 
    todays date
    
    time_collumn --- string, name of column for datetime
    """
    # getting todays date
    today = datetime.date.today()
    #checking table
    df = build_connection(self.connection.url).read().filter(sf.col(time_collumn) == today)
    check = not df.rdd.isEmpty()
    del df
    return check
    


class StrtoFilter(object):
  """
  Mostly a placeholder for now, but Bishop expects to add more
  complex functionality here in the future.
  """

  def build(self, filter_str, *largs, **kwargs):
    """
    Barebones builder, just returns filter string
    """

    return filter_str


class DataSourceBuilder(BaseBuilder):
    """
    Build a data source from URL and filter specs

    filter_spec can (currently) only be a string, but
    Bishop has plans to extension soon.
    """

    def build(self, url: str, filter_spec: str=None) -> BaseConnection:
        """
        Build a connection and return it
        """

        if filter_spec is not None:
          # Simple tuple for now, we can expand this later
          filter_key = (type(filter_spec).__name__, )
          logging.debug(f'Filter key: {filter_key}')

          # Get the handler required for specified filter
          filter_builder = self.get_handler(filter_key)

          # Now we have the filter string we need.
          filter_str = filter_builder.build(filter_spec)
        else:
          filter_str = None

        logging.info(f"Filter: {filter_str}")

        # XXX Add checks in to ensure we have specific-enough information
        # for the checks to work properly
        #
        # Glossing this for now, but we need to return to this.
        connection = build_connection(url)
        
        # Build the data source
        data_source = DataSource(connection, filter_str)
        
        return data_source


# Assume strings are already what we need, just return it
data_source_builder = DataSourceBuilder()
data_source_builder.register(('str', ), StrtoFilter)

def build_data_source(url, filter_spec, *largs, **kwargs):
  """
  Build data sources
  """

  data_source = data_source_builder.build(url, filter_spec)

  return data_source
