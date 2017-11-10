import logging

import os

_log = logging.getLogger("varscrap")


def run(scrape, input_csv_file, output_folder, overwrite=False):
    if scrape.lower() == 'vanda':
        from .scrapers.v_and_a import VandA as Scraper
        _log.info("Using V&A interface")
    elif scrape.lower() == 'wallace':
        from .scrapers.wallace_collection import WallaceCollection as Scraper
        _log.info("Using WallaceCollection interface")
    else:
        _log.error("Using an interface that is not supported.")
        raise ValueError(f"This scraper is unsupported: '{scrape}'")

    output_folder = os.path.join(output_folder, scrape)

    scraper = Scraper()
    scraper.scrape(csv_file=input_csv_file, output=output_folder, overwrite=overwrite)
