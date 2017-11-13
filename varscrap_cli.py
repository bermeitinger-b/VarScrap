import argparse
import logging

import varscrap

if __name__ == '__main__':
    cli = argparse.ArgumentParser()

    cli.add_argument(
        "-s", "--scrape"
    )

    cli.add_argument(
        "-ll", "--loglevel",
        help="Set the logging level",
        choices=['debug', 'info', 'warning', 'error'],
        default='info'
    )

    cli.add_argument(
        "-in", "--input-file",
        help="Set the input csv file as exported by Zotero",
        required=True
    )

    cli.add_argument(
        "-o", "--output",
        help="Where the output should be written to",
        required=True
    )

    cli.add_argument(
        "--overwrite",
        help="Set to true to overwrite the current folder",
        default=False
    )

    args = cli.parse_args()

    log_conf = dict(
        level=getattr(logging, args.loglevel.upper(), None),
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )

    logging.basicConfig(**log_conf)

    log = logging.getLogger(__name__)

    log.info(f"Running scraping with the options: "
             f"scrape={args.scrape} "
             f"input-file={args.input_file} "
             f"output={args.output} "
             f"overwrite={args.overwrite} ")

    varscrap.run(
        scrape=args.scrape,
        input_file=args.input_file,
        output_folder=args.output,
        overwrite=args.overwrite
    )
