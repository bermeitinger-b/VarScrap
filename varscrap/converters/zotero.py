import re


class ZoteroData(object):
    def __init__(self, object_id: str, title: str, tag: str):
        self.__object_id = object_id
        self.__title = title
        self.__tag = tag

    @property
    def object_id(self):
        return self.__object_id

    @property
    def title(self):
        return self.__title

    @property
    def tag(self):
        return self.__tag


def parse_row(row, pattern) -> ZoteroData:
    return ZoteroData(
        object_id=_extract_item_id(row['Url'], pattern),
        title=row['Title'],
        tag=row['Manual Tags']
    )


def _extract_item_id(url: str, pattern: str):
    matches = re.search(pattern, url)
    if matches:
        return matches.groupdict()["objectId"]
    else:
        raise ValueError("Cannot find object id in url")
