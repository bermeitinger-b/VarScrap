import json
import logging
import os
from typing import Optional, List
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

import pandas as pd
import requests
from lxml import html

from . import Scraper


class HermitageMuseumInformation(object):
    def __init__(self, object_id: str, title: str, inventory_nr: str, place: Optional[str] = None,
                 workshop: Optional[str] = None, date: Optional[str] = None, author: Optional[str] = None,
                 authors: Optional[str] = None, school: Optional[str] = None, material: Optional[str] = None,
                 technique: Optional[str] = None, dimensions: Optional[str] = None, category: Optional[str] = None,
                 collection: Optional[str] = None, sub_collection: Optional[str] = None,
                 image_url: Optional[str] = None):
        self.__object_id = object_id
        self.__author = author
        self.__authors = authors
        self.__title = title
        self.__place = place
        self.__workshop = workshop
        self.__date = date
        self.__school = school,
        self.__material = material
        self.__technique = technique
        self.__dimensions = dimensions
        self.__inventory_nr = inventory_nr
        self.__category = category
        self.__collection = collection
        self.__sub_collection = sub_collection
        self.__image_url = image_url
        self.__tag = None

    @property
    def object_id(self):
        return self.__object_id

    @property
    def author(self):
        return self.__author

    @property
    def authors(self):
        return self.__authors

    @property
    def title(self):
        return self.__title

    @property
    def place(self):
        return self.__place

    @property
    def workshop(self):
        return self.__workshop

    @property
    def date(self):
        return self.__date

    @property
    def school(self):
        return self.__school

    @property
    def material(self):
        return self.__material

    @property
    def technique(self):
        return self.__technique

    @property
    def dimensions(self):
        return self.__dimensions

    @property
    def inventory_nr(self):
        return self.__inventory_nr

    @property
    def category(self):
        return self.__category

    @property
    def collection(self):
        return self.__collection

    @property
    def sub_collection(self):
        return self.__sub_collection

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
            'author': self.author,
            'authors': self.authors,
            'title': self.title,
            'place': self.place,
            'workshop': self.workshop,
            'date': self.date,
            'school': self.school,
            'material': self.material,
            'technique': self.technique,
            'dimensions': self.dimensions,
            'inventory_nr': self.inventory_nr,
            'category': self.category,
            'collection': self.collection,
            'sub_collection': self.sub_collection,
            'image_url': self.image_url,
            'image_name': self.image_name,
            'tag': self.tag
        }


class HermitageMuseum(Scraper):
    """
    A scraper for the State Hermitage Museum at
    https://www.hermitagemuseum.org/

    Contrary to the other scrapers, the input is a url to the search page with encoded search request.
    Example:
    https://www.hermitagemuseum.org/wps/portal/hermitage/woa-search/?lng=en#meta_author=Hau%2C%20Edward.%201807-1887&meta_authoring_template=WOA
    Where the search request are all work of arts from the author Edward Hau in the Hermitage Collection.
    From this page, all results are scraped with selenium, as the pagination for the result does rely on Java Script.
    """

    __URL_PREFIX = "https://www.hermitagemuseum.org/"
    __XPATH_table_format = "div[{0}]/div[{1}]/{2}/text()"
    __XPATH_table = "//section[@class='her-data-table']"
    __XPATH_image_url = "/html/body/div/div[2]/div[3]/div[2]/div/div/section/div[2]/div[3]/div[1]/section/div/div[1]/div/div/div/img/@src"
    __keys = {"Author:": "author",
              "Authors:": "authors",
              "Title:": "title",
              "Place:": "place",
              "Place of creation:": "place",
              "Manufacture, workshop, firm:": "workshop",
              "Date:": "date",
              "School:": "school",
              "Material:": "material",
              "Technique:": "technique",
              "Dimensions:": "dimensions",
              "Inventory Number:": "inventory_nr",
              "Category:": "category",
              "Collection:": "collection",
              "Subcollection:": "sub_collection"}

    def __init__(self):
        self.__logger = logging.getLogger(__name__)

    @property
    def _log(self):
        return self.__logger

    def scrape(self, **kwargs):
        self._log.debug("Called scrape with options: %s", kwargs)

        objects: List[str] = self._extract_all_from_search(kwargs['input_file'])

        download_progress_file = os.path.join(kwargs['output'], "downloaded.txt")

        if os.path.isfile(download_progress_file):
            with open(download_progress_file, 'r') as fi:
                download_progress = list(set([l.strip() for l in fi]))
        else:
            download_progress = []

        annotations = []

        for (obj, obj_id) in [(o, o.split("/digital-collection/")[1].replace("/","_")) for o in objects if
                              o.split("/digital-collection/")[1].replace("/","_") not in download_progress]:
            annotation: Optional[HermitageMuseumInformation] = self.__extract_page(obj, obj_id, kwargs['output'])
            if annotation is None:
                self._log.error(f"Object '{obj_id}' could not be downloaded")
                continue
            annotations.append(annotation)
            download_progress.append(obj_id)
            with open(download_progress_file, 'w') as fo:
                fo.write("\n".join(download_progress))

        df = pd.DataFrame(
            [a.to_dict() for a in annotations],
            index=[o.split("/digital-collection/")[1].replace("/", "_") for o in objects],
            columns=['object_id', 'tag', 'image_name'])

        df.to_csv(os.path.join(kwargs['output'], "hermitage_museum_annotation.csv"))

    def __extract_page(self, obj, obj_id, output) -> Optional[HermitageMuseumInformation]:
        """
        Scraps all information from a work of art in the Hermitage Collection.

        :param obj: url to the result to be scraped
        :param obj_id: identifier of the result
        :param output: output folder where all results are saved
        :return: information from the scraped page
        :rtype: HermitageMuseumInformation
        """

        self._log.debug("Will scrape object_id '%s'", obj_id)
        page = requests.get(obj, cookies={})

        if page.ok:
            html_page = html.fromstring(page.text)
            values = {}
            i = 1
            table = html_page.xpath(self.__XPATH_table)[0]
            while True:
                key_list = table.xpath(self.__XPATH_table_format.format(i, 1, "p"))
                if len(key_list) == 0:
                    break
                key = key_list[0].replace("\n", "").rstrip(" ")
                if key not in self.__keys.keys():
                    i = i + 1
                    continue
                key = self.__keys[key]
                value_list = table.xpath(self.__XPATH_table_format.format(i, 2, "a"))
                if len(value_list) == 0:
                    value_list = table.xpath(self.__XPATH_table_format.format(i, 2, "p"))
                values[key] = value_list[0].strip("\n").rstrip(" ")
                i = i + 1
            values['image_url'] = self.__URL_PREFIX + html_page.xpath(self.__XPATH_image_url)[0]

            info = HermitageMuseumInformation(object_id=obj_id, **values)
            info.tag = ""

            with open(os.path.join(output, f"{info.object_id}.json"), 'w') as fo:
                json.dump(info.to_dict(), fo, indent=2)

            target_image = os.path.join(output, info.image_name)
            if not os.path.isfile(target_image):
                HermitageMuseum._download_image(image_url=info.image_url,
                                                target_file=target_image,
                                                cookies=page.cookies)

            return info

    def _extract_all_from_search(self, search_url):
        """
        This method extracts all result urls from a search request to the hermitage museum collection.
        This is done via selenium as the search page uses java script for pagination.

        :param search_url: url to the hermitage search page with the encoded search request
        :type search_url: str
        :return: list of all url's to the results of the search request
        :rtype: list
        """

        browser = webdriver.Firefox()
        browser.get(search_url)
        timeout = 5
        max_page = 1
        WebDriverWait(browser, timeout).until(EC.presence_of_element_located((By.CLASS_NAME, "her-pagination")))
        pagination = browser.find_element_by_class_name("her-pagination")
        li_elements = pagination.find_elements_by_tag_name("li")
        for li_element in li_elements:
            try:
                value = int(li_element.text)
                if value > max_page:
                    max_page = value
            except ValueError:
                pass
        all_links = []
        for i in range(1, max_page + 1):
            row_elements = browser.find_elements_by_class_name("her-search-results-row")
            for element in row_elements:
                link = element.find_element_by_tag_name("a").get_attribute("href")
                all_links.append(link)
                self._log.debug(link)
            pagination = browser.find_element_by_class_name("her-pagination")
            li_elements = pagination.find_elements_by_tag_name("li")
            for li_element in li_elements:
                try:
                    value = int(li_element.text)
                    if value == i + 1:
                        li_element.click()
                        WebDriverWait(browser, timeout).until(EC.staleness_of(li_element))
                        WebDriverWait(browser, timeout).until(
                            EC.presence_of_element_located((By.CLASS_NAME, "her-pagination")))
                        break
                except ValueError:
                    pass
        browser.quit()
        return all_links
