"""
Entry point for the Kids First Data Ingest Client
"""
import inspect

import click

from kf_lib_data_ingest.config import DEFAULT_TARGET_URL

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """
    A CLI utility for ingesting study data into the Kids First ecosystem.

    This method does not need to be implemented. cli is the root group that all
    subcommands will implicitly be part of.
    """
    pass


@click.command()
@click.option('--write_output',
              default=False,
              is_flag=True,
              help='A flag specifying whether to write stage output to file')
@click.option('--use_async',
              default=False,
              is_flag=True,
              help='A flag specifying whether to use sync or async loading '
              'into the target service')
@click.option('--auto_transform',
              default=False,
              is_flag=True,
              help='A flag specifying whether to use auto transformation or '
              'user guided transformation')
@click.option('-t', '--target_url',
              default=DEFAULT_TARGET_URL, show_default=True,
              help='Target service URL where data will be loaded into')
@click.argument('dataset_ingest_config_path',
                type=click.Path(exists=True, file_okay=True, dir_okay=True))
def ingest(dataset_ingest_config_path, target_url, auto_transform, use_async,
           write_output):
    """
    Run the Kids First data ingest pipeline.

    \b
    Arguments:
        \b
        dataset_ingest_config_path - the path to the data ingest config file
        or a path to a directory which contains a file called
        'dataset_ingest_config_path.yml'
    """
    import os
    from kf_lib_data_ingest.etl.ingest_pipeline import DataIngestPipeline

    # Make kwargs from options
    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    kwargs = {arg: values[arg] for arg in args[1:]}

    # Make path to the Kids First target api config
    root_dir = os.path.abspath(os.path.dirname(__file__))
    target_api_config_path = os.path.join(
        root_dir, 'target_apis', 'kids_first.py')

    # Construct ingest pipeline
    p = DataIngestPipeline(dataset_ingest_config_path)

    # Run ingest
    p.run(target_api_config_path, **kwargs)


@click.command()
def extract():
    pass


@click.command()
def transform():
    pass


@click.command()
def load():
    pass


cli.add_command(ingest)
