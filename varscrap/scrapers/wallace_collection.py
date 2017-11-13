import json
import logging
import os
import re
from typing import Optional

import requests
from scrapy import Selector

from . import Scraper

logging.getLogger("scrapy.core.engine").setLevel(logging.WARNING)


class WallaceCollectionInformation(object):
    def __init__(self, object_id: str, object_name: str, title: str, reference: str, reference_data: str,
                 place_artist: str, dates_all: str, material: str, dimensions: str, marks: str, museum_number: str,
                 commentary: str, image_url: Optional[str] = None):
        self.__object_id = object_id
        self.__object_name = object_name
        self.__title = title
        self.__reference = reference
        self.__reference_data = reference_data
        self.__place_artist = place_artist
        self.__dates_all = dates_all
        self.__material = material
        self.__dimensions = dimensions
        self.__marks = marks
        self.__museum_number = museum_number
        self.__commentary = commentary
        self.__image_url = image_url

    @property
    def object_id(self):
        return self.__object_id

    @property
    def object_name(self):
        return self.__object_name

    @property
    def title(self):
        return self.__title

    @property
    def reference(self):
        return self.__reference

    @property
    def reference_data(self):
        return self.__reference_data

    @property
    def place_artist(self):
        return self.__place_artist

    @property
    def dates_all(self):
        return self.__dates_all

    @property
    def material(self):
        return self.__material

    @property
    def dimensions(self):
        return self.__dimensions

    @property
    def marks(self):
        return self.__marks

    @property
    def museum_number(self):
        return self.__museum_number

    @property
    def commentary(self):
        return self.__commentary

    @property
    def image_url(self):
        return self.__image_url

    @image_url.setter
    def image_url(self, image_url):
        self.__image_url = image_url

    def to_dict(self):
        return {
            'object_id': self.object_id,
            'object_name': self.object_name,
            'title': self.title,
            'reference': self.reference,
            'reference_data': self.__reference_data,
            'place_artist': self.place_artist,
            'dates_all': self.dates_all,
            'material': self.material,
            'dimensions': self.dimensions,
            'marks': self.marks,
            'museum_number': self.museum_number,
            'commentary': self.commentary,
            'image_url': self.image_url
        }


class WallaceCollection(Scraper):
    """
    A scraper for the Wallace Collection at
    http://wallacelive.wallacecollection.org/
    """

    __URL_PREFIX = "http://wallacelive.wallacecollection.org"
    __URL_TEMPLATE = "/eMuseumPlus?service=ExternalInterface&module=collection&viewType=detailView&objectId="
    __IMAGE_SUFFIX = ".jpg"

    __XPATH = {
        'object_name': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dd[1]/ul[1]/li[1]/span[1]/text()',
        'title': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dd[1]/ul[1]/li[2]/span[1]/text()',
        'reference': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dd[1]/ul[1]/li[3]/span/span/a/span/text()',
        'reference_data': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dd[1]/ul[1]/li[4]/span[1]/text()',
        'place_artist': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dd[1]/ul[1]/li[5]/span[1]/text()',
        'dates_all': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dd[1]/ul[1]/li[6]/span[1]/text()',
        'material': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dd[1]/ul[1]/li[7]/span[1]/text()',
        'dimensions': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dd[1]/ul[1]/li[8]/span[1]/text()',
        'marks': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dd[1]/ul[1]/li[9]/span[1]/text()',
        'museum_number': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dd[1]/ul[1]/li[10]/span[1]/text()',
        'commentary': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[2]/dd/div/ul/li/span[1]/text()',
        'image_url': '/html/body/div[1]/div[4]/div[2]/div[2]/dl[1]/dt[1]/a/@href'
    }

    def __init__(self):
        self.__logger = logging.getLogger(self.__class__.__name__)

    @property
    def _log(self):
        return self.__logger

    def scrape(self, **kwargs):
        self._log.debug("Called scrape with options: %s", kwargs)

        object_ids = self._extract_object_ids(kwargs['input_file'])

        for object_id in object_ids:
            self.__extract_page(object_id, kwargs['output'])

    def __extract_page(self, object_id, output) -> None:
        page = requests.get(
            f"{self.__URL_PREFIX}{self.__URL_TEMPLATE}{object_id}",
            cookies={}
        )

        if page.ok:
            html_page = Selector(text=page.text)

            values = {key: html_page.xpath(value).extract_first() for key, value in self.__XPATH.items()}

            info = WallaceCollectionInformation(object_id=object_id, **values)

            image_popup = requests.get(self.__URL_PREFIX + re.findall(r"(/eMuseumPlus.*=F)", values['image_url'])[0],
                                       cookies=page.cookies)
            if image_popup.ok:
                info.image_url = self.__URL_PREFIX + Selector(text=image_popup.text) \
                    .xpath("/html/body/div/table/tr/td/img/@src") \
                    .extract_first()

                with open(os.path.join(output, f"{info.object_id}.json"), 'w') as fo:
                    json.dump(info.to_dict(), fo, indent=2)
                WallaceCollection._download_image(image_url=info.image_url,
                                                  target_file=os.path.join(
                                                      output,
                                                      f"{info.object_id}{self.__IMAGE_SUFFIX}"),
                                                  cookies=image_popup.cookies)

    @staticmethod
    def _extract_object_ids(input_file):
        with open(input_file, 'r') as fi:
            text = fi.read()
        object_id_pattern = r'objectId=([0-9]+)'

        return sorted(list(set(re.findall(object_id_pattern, text))))
