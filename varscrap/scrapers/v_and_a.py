import json
import logging
import os
from typing import List, Dict

import pandas as pd
import requests

from . import Scraper
from ..converters import zotero


class ShallowVandAInformation(object):
    def __init__(self, item_id: str, tag: str):
        self.__item_id = item_id
        self.__tag = tag

    @property
    def item_id(self):
        return self.__item_id

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
            'tag': self.tag
        }


class DeepVandAInformation(ShallowVandAInformation):
    def __init__(self,
                 shallow: ShallowVandAInformation,
                 image_urls: List[str],
                 verbose: Dict):
        super().__init__(shallow.item_id, shallow.tag)
        self.__image_urls = image_urls
        self.__verbose = verbose
        self.__image_names = []

    @property
    def image_urls(self):
        return self.__image_urls

    @property
    def verbose(self):
        return self.__verbose

    @property
    def image_names(self):
        return self.__image_names

    def to_dict(self):
        d = super(DeepVandAInformation, self).to_dict()
        d.update({
            'image_urls': self.image_urls,
            'image_names': self.image_names,
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

    __IGNORED_TAGS = [
        ";"
    ]

    __OBJECT_ID_PATTERN = r'item/(?P<objectId>O[0-9]+)'

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

        if not self._check_input(**kwargs):
            raise ValueError("One or more arguments are missing.")

        self._prepare_output(output=kwargs['output'], overwrite=kwargs['overwrite'])
        self._log.info("Output folder prepared: %s", kwargs['output'])

        df = self._load_csv(kwargs['input_file'])

        data: List[ShallowVandAInformation] = []

        for _, row in df.iterrows():
            import_data: zotero.ZoteroData = zotero.parse_row(row, self.__OBJECT_ID_PATTERN)
            if any(x in import_data.tag for x in self.__IGNORED_TAGS):
                continue
            if import_data.object_id in [j.item_id for j in data]:
                self._log.debug("Duplicated object id: '%s'", import_data.object_id)
                continue
            data.append(
                ShallowVandAInformation(
                    item_id=import_data.object_id,
                    tag=import_data.tag
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
                target_file = os.path.join(kwargs['output'], f"{d.item_id}_{idx}{self.__IMAGE_SUFFIX}")

                self._log.info(f"Will download image {idx + 1}/{len(d.image_urls)} for '{d.item_id}'")
                if os.path.isfile(target_file):
                    self._log.debug("Already exists, skipping")
                else:
                    if self._download_image(
                            image_url=image_url,
                            target_file=target_file
                    ):
                        d.image_names.append(target_file)
                    else:
                        self._log.warning("Could not download this file.")

        _item_ids = []
        _tags = []
        _image_paths = []

        for d in deep_data:
            for ip in d.image_names:
                _item_ids.append(d.item_id)
                _tags.append(d.tag)
                _image_paths.append(ip)

        df = pd.DataFrame(
            {
                'item_id': _item_ids,
                'tag': _tags,
                'image_path': _image_paths
            }
        )

        df.to_csv(os.path.join(kwargs['output'], 'vanda_scraped.csv'))

    def _check_input(self, **kwargs) -> bool:
        return super(VandA, self)._check_input(kwargs) and all(x in kwargs for x in self.__special_input)

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
