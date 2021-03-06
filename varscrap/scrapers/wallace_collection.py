import json
import logging
import os
import re
from typing import Optional, List

import pandas as pd
import requests
from lxml import html

from . import Scraper
from ..converters.zotero import ZoteroData, parse_row


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
        self.__tag = None

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

    @property
    def image_name(self):
        return f"{self.object_id}.jpg"

    @property
    def tag(self):
        return self.__tag

    @tag.setter
    def tag(self, tag):
        self.__tag = tag

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
            'image_url': self.image_url,
            'image_name': self.image_name,
            'tag': self.tag
        }


class WallaceCollection(Scraper):
    """
    A scraper for the Wallace Collection at
    http://wallacelive.wallacecollection.org/
    """

    __URL_PREFIX = "http://wallacelive.wallacecollection.org"
    __URL_TEMPLATE = "/eMuseumPlus?service=ExternalInterface&module=collection&viewType=detailView&objectId="

    __URL_OBJECT_ID = r"objectId=(?P<objectId>[0-9]+)"

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
        self.__logger = logging.getLogger(__name__)

    @property
    def _log(self):
        return self.__logger

    def scrape(self, **kwargs):
        self._log.debug("Called scrape with options: %s", kwargs)

        objects: List[ZoteroData] = [
            parse_row(row, self.__URL_OBJECT_ID)
            for _, row in pd.read_csv(kwargs['input_file']).iterrows()
        ]

        download_progress_file = os.path.join(kwargs['output'], "downloaded.txt")

        if os.path.isfile(download_progress_file):
            with open(download_progress_file, 'r') as fi:
                download_progress = list(set([l.strip() for l in fi]))
        else:
            download_progress = []

        annotations = []

        for obj in [o for o in objects if o.object_id not in download_progress]:
            annotation: Optional[WallaceCollectionInformation] = self.__extract_page(obj, kwargs['output'])
            if annotation is None:
                self._log.error(f"Object '{obj.object_id}' could not be downloaded")
                continue
            annotations.append(annotation)
            download_progress.append(obj.object_id)
            with open(download_progress_file, 'w') as fo:
                fo.write("\n".join(download_progress))

        df = pd.DataFrame(
            [a.to_dict() for a in annotations],
            index=[o.object_id for o in objects],
            columns=['object_id', 'tag', 'image_name'])

        df.to_csv(os.path.join(kwargs['output'], "wallace_annotation.csv"))

    def __extract_page(self, obj: ZoteroData, output) -> Optional[WallaceCollectionInformation]:
        self._log.debug("Will scrape object_id '%s'", obj.object_id)
        page = requests.get(
            f"{self.__URL_PREFIX}{self.__URL_TEMPLATE}{obj.object_id}",
            cookies={}
        )

        if page.ok:
            html_page = html.fromstring(page.text)

            values = {}
            for xpath_key, xpath_string in self.__XPATH.items():
                xpath = html_page.xpath(xpath_string)
                values[xpath_key] = xpath[0] if len(xpath) > 0 else ""

            info = WallaceCollectionInformation(object_id=obj.object_id, **values)
            info.tag = obj.tag

            image_popup = requests.get(self.__URL_PREFIX + re.findall(r"(/eMuseumPlus.*=F)", values['image_url'])[0],
                                       cookies=page.cookies)
            if image_popup.ok:
                info.image_url = self.__URL_PREFIX + html.fromstring(image_popup.text) \
                    .xpath("/html/body/div/table/tr/td/img/@src")[0]

                with open(os.path.join(output, f"{info.object_id}.json"), 'w') as fo:
                    json.dump(info.to_dict(), fo, indent=2)

                target_image = os.path.join(output, f"{info.object_id}.jpg")
                if not os.path.isfile(target_image):
                    WallaceCollection._download_image(image_url=info.image_url,
                                                      target_file=target_image,
                                                      cookies=image_popup.cookies)

                return info

    @staticmethod
    def _extract_object_ids(input_file):
        with open(input_file, 'r') as fi:
            text = fi.read()
        object_id_pattern = r'objectId=([0-9]+)'

        return sorted(list(set(re.findall(object_id_pattern, text))))
