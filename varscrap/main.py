import logging

import os

_log = logging.getLogger(__name__)


def run(scrape, input_file, output_folder, overwrite=False):
    if scrape.lower() == 'vanda':
        from .scrapers.v_and_a import VandA as Scraper
        _log.info("Using V&A interface")
    elif scrape.lower() == 'wallace':
        from .scrapers.wallace_collection import WallaceCollection as Scraper
        _log.info("Using WallaceCollection interface")
    elif scrape.lower() == 'hermitagemuseum':
         from .scrapers.state_hermitage_museum import HermitageMuseum as Scraper
         _log.info("Usin HermitageMuseum interface")
    else:
        _log.error("Using an interface that is not supported.")
        raise ValueError(f"This scraper is unsupported: '{scrape}'")

    if not os.path.isdir(output_folder):
        os.makedirs(output_folder, exist_ok=True)

    scraper = Scraper()
    scraper.scrape(input_file=input_file, output=output_folder, overwrite=overwrite)
