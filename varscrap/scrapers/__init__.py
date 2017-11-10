import logging
import os
from abc import ABC, abstractmethod

import pandas as pd
import requests

logging.getLogger("urllib3").setLevel(logging.WARNING)


class Scraper(ABC):

    @property
    @abstractmethod
    def _log(self):
        pass

    @abstractmethod
    def scrape(self, **kwargs):
        pass

    @staticmethod
    def _load_csv(csv_file: str) -> pd.DataFrame:
        df = pd.read_csv(
            csv_file,
            delimiter=',',
            index_col=['Key'],
            na_filter=False
        )

        return df

    @staticmethod
    def _check_input(kwargs) -> bool:
        return all(x in kwargs for x in ['csv_file', 'output', 'overwrite'])

    @staticmethod
    def _prepare_output(output: str, overwrite: bool = False):
        if overwrite:
            from shutil import rmtree
            rmtree(output)

        if not os.path.isdir(output):
            os.makedirs(output, exist_ok=True)

    def _download_image(self, image_url: str, target_file: str) -> None:
        r = requests.get(image_url, stream=True)
        if r.ok:
            with open(target_file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
        else:
            self._log.error("Could not download image '{}': Code {}".format(image_url, r.status_code))
