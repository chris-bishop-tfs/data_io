"""Location classes used to describe data source"""
import abc
import re
from attrs import define
from .builder import URLKeyBuilder
from urllib.parse import urlparse
import urllib

def parse_password(url):
    # More comprehensive regex to match from first ':' after scheme to last '@'
    # This handles URLs with complex passwords and multiple potential '@' characters
    match = re.search(r'://[^:]+:(.*?)(?=@[^@]*$)', url)
    
    if match:
        # Extract and decode the password
        full_password = match.group(1)
        return full_password
    return None

@define
class Location(abc.ABC):
    """
    Base Location object.
    
    Note that this was originally a minor extension of urlpath's URL class, but changes in python 3.12 nuked this class.

    Bishop instead wrote a barebones class that can standalone over time.

    Not terribly elegant, but more robust.

    Don't like it? Write your own :)
    """

    url: str = None

    def __repr__(self):
        return self.url

    def _unquote(self, result):
        """
        URLs are special character encoded ("%" encoded)
        Use this function to undo that
        """
        return urllib.parse.unquote(result)

    @property
    def _parsed_url(self):
        return urlparse(self.url)

    @property
    def scheme(self):
        return self._parsed_url.scheme
  
    @property
    def username(self):
        # Usernames can be encoded - so unencode it
        return self._unquote(self._parsed_url.username)

    @property
    def hostname(self):
        return self._parsed_url.hostname

    @property
    def password(self):
        # Can be encoded - so unencode it
        return self._unquote(parse_password(self.url))


    @property
    def scheme(self):
        return self._parsed_url.scheme
  
    @property
    def port(self):
        return self._parsed_url.port
    
    @property
    def path(self):
        return self._parsed_url.path


class LocationBuilder(URLKeyBuilder):
    """
    Location Builder generates a location from a fully qualified URL
    """

    def __init__(self, *largs, **kwargs):
        super(URLKeyBuilder, self).__init__()

    def build(self, url: str) -> Location:
        '''
            default ports in builder
        '''
        # dictionary of defaul port
        default_port = dict(oracle='1521', 
        redshift='5439')

        path = urlparse(url)

        #checking if port is empty
        if((path.port == None) and (path.scheme != 's3a') and (path.scheme != 's3')):
            user = urlparse(url)
            user = user._replace(netloc=
            user.netloc + ':' + default_port[user.scheme])
            url = user.geturl()
        url_key = self.build_url_key(url)

        location_class = self.get_handler(url_key)

        # Get the location object
        location = location_class(url)

        return location


class DatabaseLocation(Location):
    """
    Database connections require extra parsing
    """

    def __init__(self, url):
        super(Location, self).__init__()

        self.url = url

        # Additional parsing and attributes for
        # databases
        path_split = self.path.replace('/', '').split('.')

        # Need some conditional formatting here
        if len(path_split) == 3:
            # db, schema, table
            self.db, self.schema, self.table = path_split

        elif len(path_split) == 2:
            # db, table
            # Occurs with backends like Impala
            self.db, self.schema = path_split
            self.table = None

        # Add another use case with just the database
        elif len(path_split) == 1:
            self.db = path_split[0]
            self.schema = self.table = None
        else:
            raise NotImplementedError


# Register location building keys/classes
location_builder = LocationBuilder()
location_builder.register(('redshift',), DatabaseLocation)
location_builder.register(('oracle',), DatabaseLocation)
location_builder.register(('postgresql',), DatabaseLocation)
location_builder.register(('s3a',), Location)
location_builder.register(('s3',), Location)


def build_location(url, *largs, **kwargs):
    """
    Add abstraction for location building
    """

    return location_builder.build(url, *largs, **kwargs)
