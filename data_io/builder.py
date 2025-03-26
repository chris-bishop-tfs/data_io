"""Base builder classes"""
import abc
from typing import Union
from urllib.parse import urlparse


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

    def register(self, handler_key: str, handler) -> None:
        """
        Registers a new subclass in handlers dictionary.

        Args:
          handler_key (tuple): dictionary key
          handler (class): subclass
        """

        self.handlers[handler_key] = handler

    def get_handler(self, handler_key: str):
        """
        Find subclass based on key.

        Args:
          handler_key (tuple):

        Returns:
        """

        return self.handlers[handler_key]

    @abc.abstractmethod
    def build(self) -> None:
        """
        This is the custom piece of a builder.

        It will returned a constructed object
        """

        raise NotImplementedError


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

    def build_url_key(self, url: str) -> str:
        """
        Parse URL to create the required key
        """

        # Convert URL into a location object
        # This will be parsed, etc. correctly
        # Removed URL dependency due to deprecation
        location = urlparse(url)

        # Build the connection key from the URL
        #  scheme (protocol), extension?
        #
        #  KISS and use protocol only to start
        url_key = (location.scheme,)
        
        return url_key
