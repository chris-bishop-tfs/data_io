"""Location classes used to describe data source"""
from .builder import URLKeyBuilder
from .builder import URL


class Location(URL):
    """
    Store URL information, will likely need to extend this
    to do some custom things later. At the moment, just a
    stub for extension
    """

    def __init__(self, *largs, **kwargs):
        super(URL, self).__init__()

    pass


class LocationBuilder(URLKeyBuilder):
    """
    Location Builder generates a location from a fully qualified URL
    """

    def __init__(self, *largs, **kwargs):
        super(URLKeyBuilder, self).__init__()

    def build(self, url: str) -> Location:

        # Build the URL key
        url_key = self.build_url_key(url)

        location_class = self.get_handler(url_key)

        # Get the location object
        location = location_class(url)

        return location


class DatabaseLocation(Location):
    """
    Database connections require extra parsing
    """

    def __init__(self, *largs, **kwargs):
        super(Location, self).__init__()

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
            self.db, self.table = path_split
            self.schema = None

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


def build_location(url, *largs, **kwargs):
    """
    Add abstraction for location building
    """

    return location_builder.build(url, *largs, **kwargs)
