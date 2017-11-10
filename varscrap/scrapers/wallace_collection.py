import logging

from . import Scraper


class WallaceCollection(Scraper):
    """
    A scraper for the Wallace Collection at
    http://wallacelive.wallacecollection.org/
    """

    def __init__(self):
        self.__logger = logging.getLogger(self.__class__.__name__)

    @property
    def _log(self):
        return self.__logger

    def scrape(self, **kwargs):
        self._log.debug("Called scrape with options: %s", kwargs)
