# VarScrap
Tool for scraping various online material

# Hermitage Museum Scraper
To access the scraper, start varscrap_cli.py with the option "-s hermitagemuseum".
The input now needs to be an URL to the search request to be scraped.
For example:
"-in https://www.hermitagemuseum.org/wps/portal/hermitage/woa-search/?lng=en#meta_author=Hau%2C%20Edward.%201807-1887&meta_authoring_template=WOA"
which scraps all work of arts in the Hermitage Collection from Edward Hau.
Furthermore, as the search is paginated with java script, selenium is used to iterate over all results.
Therefore, the geckodriver needs to be downloaded and added to PATH.

Every downloaded item will be stored as id.jpg or id.json.
The id is taken from the URL from the downloaded item. This URL is always:
https://www.hermitagemuseum.org/wps/portal/hermitage/digital-collection/TYPEOFWOA/NUMBER
Therefore the id is given as TYPEOFWOA_NUMBER and the URL can be restored if the underscore is replaced with a slash.

