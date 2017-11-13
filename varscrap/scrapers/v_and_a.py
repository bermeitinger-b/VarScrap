import json
import logging
import os
from typing import List, Optional, Dict
from urllib.parse import urlparse

import requests

from . import Scraper


class ShallowVandAInformation(object):
    def __init__(self, item_id: str, url: str, title: str, abstract_note: str, tag: str):
        if any(x is None for x in [item_id, url, title, abstract_note, tag]):
            raise ValueError("You must set all initial parameters.")

        # Must have values from Zotero export
        self.__item_id = item_id
        self.__url = url
        self.__title = title
        self.__abstract_note = abstract_note
        self.__tag = tag

    @property
    def item_id(self):
        return self.__item_id

    @property
    def url(self):
        return self.__url

    @property
    def title(self):
        return self.__title

    @property
    def abstract_note(self):
        return self.__abstract_note

    @property
    def tag(self):
        return self.__tag

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self):
        return f"ShallowVandAInformation: {self.item_id}, {self.tag}"

    def to_dict(self) -> Dict:
        return {
            'item_id': self.item_id,
            'url': self.url,
            'title': self.title,
            'abstract_note': self.abstract_note,
            'tag': self.tag
        }


class DeepVandAInformation(ShallowVandAInformation):

    def __init__(self,
                 shallow: ShallowVandAInformation,
                 image_urls: List[str],
                 verbose: Dict):
        super().__init__(shallow.item_id, shallow.url, shallow.title, shallow.abstract_note, shallow.tag)
        self.__image_urls = image_urls
        self.__verbose = verbose

    @property
    def image_urls(self):
        return self.__image_urls

    @property
    def verbose(self):
        return self.__verbose

    def to_dict(self):
        d = super(DeepVandAInformation, self).to_dict()
        d.update({
            'image_urls': self.image_urls,
            'verbose': self.verbose
        })
        return d


class VandA(Scraper):
    """
    A scraper for the V&A collection:
    http://www.vam.ac.uk

    It's using the available API from: http://www.vam.ac.uk/api/
    """
    __special_input = []

    def __init__(self):
        self.__logger = logging.getLogger(self.__class__.__name__)

    @property
    def _log(self):
        return self.__logger

    # noinspection PyPep8Naming
    @property
    def __API_URL(self):
        return "http://www.vam.ac.uk/api/json/museumobject"

    # noinspection PyPep8Naming
    @property
    def __IMAGE_URL(self):
        return "http://media.vam.ac.uk/media/thira/collection_images"

    # noinspection PyPep8Naming
    @property
    def __IMAGE_SUFFIX(self):
        return ".jpg"

    def scrape(self, **kwargs):
        self._log.debug("Called scrape with options: %s", kwargs)

        if not self._check_input(kwargs):
            raise ValueError("One or more arguments are missing.")

        self._prepare_output(output=kwargs['output'], overwrite=kwargs['overwrite'])
        self._log.info("Output folder prepared: %s", kwargs['output'])

        df = self._load_csv(kwargs['input_file'])

        data: List[ShallowVandAInformation] = []

        for _, row in df.iterrows():
            data.append(
                ShallowVandAInformation(
                    item_id=self.__extract_item_id(row['Url']),
                    url=row['Url'],
                    title=row['Title'],
                    abstract_note=row['Abstract Note'],
                    tag=self.__extract_tag(row['Manual Tags'], row['Title'])
                )
            )

        self._log.info("Found %s item ids", len(data))
        if self._log.isEnabledFor(logging.DEBUG):
            self._log.debug("Item IDs: \n%s", "\n".join(str(x) for x in data))

        self._log.info("Will call API for each element to get images and additional information")
        deep_data: List[DeepVandAInformation] = [
            self.__call_api(d) for d in data
        ]

        self._log.info("Saving json files")
        for d in deep_data:
            with open(os.path.join(kwargs['output'], f"{d.item_id}.json"), 'w') as fo:
                json.dump(d.to_dict(), fo, indent=2)

        self._log.info("Downloading images")
        for d in deep_data:
            for idx, image_url in enumerate(d.image_urls):
                self._download_image(
                    image_url=image_url,
                    target_file=os.path.join(kwargs['output'], f"{d.item_id}_{idx}{self.__IMAGE_SUFFIX}")
                )

    def _check_input(self, kwargs) -> bool:
        return super(VandA, self)._check_input(kwargs) and all(x in kwargs for x in self.__special_input)

    @staticmethod
    def __extract_item_id(url_str) -> str:
        url = urlparse(url_str)
        path = url.path.split("/")
        assert path[0] == ''
        assert path[1] == 'item'
        return path[2]

    @staticmethod
    def __extract_tag(manual_tag: str, title: Optional[str]) -> str:
        if len(manual_tag) > 0:
            return manual_tag
        else:
            return title.split("|")[0]

    def __call_api(self, source: ShallowVandAInformation) -> DeepVandAInformation:
        req = requests.get(f"{self.__API_URL}/{source.item_id}")

        if not req.ok:
            raise InterruptedError(req.status_code)

        data = req.json()
        assert len(data) == 1
        data = data[0]['fields']

        assert data['object_number'] == source.item_id

        image_ids = [data['primary_image_id']]
        image_ids.extend([
            x['fields']['image_id'] for x in data['image_set'] if x['fields']['image_id'] not in image_ids
        ])

        image_urls = [
            f"{self.__IMAGE_URL}/{i[:6]}/{i}{self.__IMAGE_SUFFIX}"
            for i in image_ids]

        obj = DeepVandAInformation(shallow=source, image_urls=image_urls, verbose=data)

        return obj
