"""
Entry point for the Kids First Data Ingest Client
"""
import os
import inspect

import click

from etl.ingest_pipeline import DataIngestPipeline
from config import DEFAULT_TARGET_URL

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
TARGET_API_CONFIG_PATH = os.path.join(ROOT_DIR, 'target_apis', 'kids_first.py')

# Params needed to construct common click.options
DATASET_INGEST_CONFIG_PARAMS = {
    'args': ['dataset_ingest_config_path'],
    'kwargs': {
        'type': click.Path(exists=True, file_okay=True, dir_okay=True)
    },
    'help': """
    \b
    Arguments:
        \b
        dataset_ingest_config_path - the path to the data ingest config file
        or a path to a directory which contains a file called
        'dataset_ingest_config_path.yml'
    """
}
USE_ASYNC_PARAMS = {
    'args': ['--use_async'],
    'kwargs': {
        'default': False,
        'show_default': True,
        'is_flag': True,
        'help': 'A flag specifying whether to use sync or async loading '
        'into the target service'
    }
}
TARGET_URL_PARAMS = {
    'args': ['--target_url'],
    'kwargs': {
        'default': DEFAULT_TARGET_URL,
        'show_default': True,
        'help': 'Target service URL where data will be loaded into'
    }
}
INPUT_DIR_PARAMS = {
    'args': ['-i', '--input_dir'],
    'kwargs': {
        'help': 'The directory where stage input will be loaded from. If not '
        'specified, input will be loaded from "output/<stage>" inside the '
        'directory of <dataset_ingest_config_path>. <stage> is either '
        '"extract", "transform", or "load"',
        'type': click.Path(file_okay=False, exists=True, dir_okay=True)
    }
}

OUTPUT_DIR_PARAMS = {
    'args': ['-o', '--output_dir'],
    'kwargs': {
        'help': 'The directory where stage output will be stored. '
        'If not specified, output will be written to "output/<stage>" '
        'inside the directory of <dataset_ingest_config_path>. <stage> is '
        'either "extract", "transform", or "load"',
        'type': click.Path(file_okay=False, dir_okay=True)
    }
}

OVERWRITE_PARAMS = {
    'args': ['--overwrite'],
    'kwargs': {
        'default': True,
        'show_default': True,
        'is_flag': True,
        'help': 'A flag specifying whether stage output should be overwritten '
        'or new files should be written in the output directory.'
    }
}
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
@click.option(*USE_ASYNC_PARAMS['args'], **USE_ASYNC_PARAMS['kwargs'])
@click.option(*TARGET_URL_PARAMS['args'], **TARGET_URL_PARAMS['kwargs'])
@click.argument(*DATASET_INGEST_CONFIG_PARAMS['args'],
                **DATASET_INGEST_CONFIG_PARAMS['kwargs'])
def ingest(dataset_ingest_config_path, target_url, use_async):
    """
    Run the Kids First data ingest pipeline.

    \b
    Arguments:
        \b
        dataset_ingest_config_path - the path to the data ingest config file
        or a path to a directory which contains a file called
        'dataset_ingest_config_path.yml'
    """
    op_args = [TARGET_API_CONFIG_PATH]
    op_kwargs = _kwargs_from_frame(inspect.currentframe())
    _run_pipeline('ingest', dataset_ingest_config_path, *op_args, **op_kwargs)


@click.command()
@click.option(*OVERWRITE_PARAMS['args'], **OVERWRITE_PARAMS['kwargs'])
@click.option(*OUTPUT_DIR_PARAMS['args'], **OUTPUT_DIR_PARAMS['kwargs'])
@click.argument(*DATASET_INGEST_CONFIG_PARAMS['args'],
                **DATASET_INGEST_CONFIG_PARAMS['kwargs'])
def extract(dataset_ingest_config_path, output_dir, overwrite):
    """
    Run the extract stage of the Kids First data ingest pipeline.

    \b
    Arguments:
        \b
        dataset_ingest_config_path - the path to the data ingest config file
        or a path to a directory which contains a file called
        'dataset_ingest_config_path.yml'
    """
    op_kwargs = _kwargs_from_frame(inspect.currentframe())
    _run_pipeline('extract', dataset_ingest_config_path, **op_kwargs)


@click.command()
@click.option(*OVERWRITE_PARAMS['args'], **OVERWRITE_PARAMS['kwargs'])
@click.option(*OUTPUT_DIR_PARAMS['args'], **OUTPUT_DIR_PARAMS['kwargs'])
@click.option(*INPUT_DIR_PARAMS['args'], **INPUT_DIR_PARAMS['kwargs'])
@click.argument(*DATASET_INGEST_CONFIG_PARAMS['args'],
                **DATASET_INGEST_CONFIG_PARAMS['kwargs'])
def transform(dataset_ingest_config_path, input_dir, output_dir, overwrite):
    """
    Run the transform stage of the Kids First data ingest pipeline.

    \b
    Arguments:
        \b
        dataset_ingest_config_path - the path to the data ingest config file
        or a path to a directory which contains a file called
        'dataset_ingest_config_path.yml'
    """
    op_args = [TARGET_API_CONFIG_PATH]
    op_kwargs = _kwargs_from_frame(inspect.currentframe())
    _run_pipeline('transform', dataset_ingest_config_path,
                  *op_args, **op_kwargs)


@click.command()
@click.option(*OVERWRITE_PARAMS['args'], **OVERWRITE_PARAMS['kwargs'])
@click.option(*OUTPUT_DIR_PARAMS['args'], **OUTPUT_DIR_PARAMS['kwargs'])
@click.option(*INPUT_DIR_PARAMS['args'], **INPUT_DIR_PARAMS['kwargs'])
@click.option(*USE_ASYNC_PARAMS['args'], **USE_ASYNC_PARAMS['kwargs'])
@click.option(*TARGET_URL_PARAMS['args'], **TARGET_URL_PARAMS['kwargs'])
@click.argument(*DATASET_INGEST_CONFIG_PARAMS['args'],
                **DATASET_INGEST_CONFIG_PARAMS['kwargs'])
def load(dataset_ingest_config_path, input_dir, output_dir, overwrite):
    """
    Run the load stage of the Kids First data ingest pipeline.

    \b
    Arguments:
        \b
        dataset_ingest_config_path - the path to the data ingest config file
        or a path to a directory which contains a file called
        'dataset_ingest_config_path.yml'
    """
    op_args = [TARGET_API_CONFIG_PATH]
    op_kwargs = _kwargs_from_frame(inspect.currentframe())
    _run_pipeline('load', dataset_ingest_config_path, *op_args, **op_kwargs)


def _kwargs_from_frame(current_frame, start_arg_pos=1):
    args, _, _, values = inspect.getargvalues(current_frame)
    kwargs = {arg: values[arg] for arg in args[start_arg_pos:]}

    return kwargs


def _run_pipeline(operation, dataset_ingest_config_path,
                  *op_args, **op_kwargs):
    """
    Helper method to setup and run data ingest pipeline

    Keyword args are extracted from current_frame
    """
    # Construct ingest pipeline
    p = DataIngestPipeline(dataset_ingest_config_path)

    # Run ingest
    p.run(operation, *op_args, **op_kwargs)


# Add subcommands to the cli group
cli.add_command(ingest)
cli.add_command(extract)
cli.add_command(transform)
cli.add_command(load)
