#!/usr/bin/env python

import argparse
import os

from etl.ingest_pipeline import DataIngestPipeline


def main(data_ingest_config_path, **kwargs):

    # Kidsfirst target api
    root_dir = os.path.dirname(__file__)
    target_api_config_path = os.path.join(
        os.path.abspath(root_dir),
        'target_apis',
        'kids_first.py'
    )

    # Construct ingest pipeline
    p = DataIngestPipeline(data_ingest_config_path)

    # Run
    p.run(target_api_config_path, **kwargs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("data_ingest_config_path",
                        help="Path to the data ingest config file or a "
                        "path to a directory which contains a file called "
                        "'dataset_ingest_config.yml'")
    parser.add_argument("--target_url", type=str,
                        help="URL for the target service we are loading into.")
    parser.add_argument("--use_async", action="store_true",
                        help="Whether to use asynchronous loading or"
                        " synchronous loading")
    args = parser.parse_args()

    kwargs = {
        'use_async': args.use_async
    }
    if args.target_url:
        kwargs['target_url'] = args.target_url

    main(args.data_ingest_config_path, **kwargs)
