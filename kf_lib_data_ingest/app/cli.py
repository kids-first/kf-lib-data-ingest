"""
Entry point for the Kids First Data Ingest Client
"""
import inspect
import logging
import sys

import click

from kf_lib_data_ingest.app import settings
from kf_lib_data_ingest.config import DEFAULT_LOG_LEVEL, DEFAULT_TARGET_URL
from kf_lib_data_ingest.etl.ingest_pipeline import (
    DataIngestPipeline,
    DEFAULT_STAGES_TO_RUN_STR
)

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
DEFAULT_LOG_LEVEL_NAME = logging._levelToName.get(DEFAULT_LOG_LEVEL)


def common_args_options(func):
    """
    Common click args and options
    """
    # Ingest package config
    func = click.argument('ingest_package_config_path',
                          type=click.Path(exists=True,
                                          file_okay=True,
                                          dir_okay=True))(func)
    # App settings
    func = click.option(
        '--app_settings', 'app_settings_filepath',
        type=click.Path(exists=True, file_okay=True, dir_okay=False),
        help=('Path to an ingest app settings file. If not specified, '
              'will use default app settings for the current app mode, '
              'which is specified by environment variable: '
              f'{settings.APP_MODE_ENV_VAR}. '
              'See kf_lib_data_ingest.app.settings for default settings '
              'files'))(func)

    # Log level
    log_help_txt = (
        'Controls level of log messages to output. If not supplied via CLI, '
        'log_level from the ingest_package_config.py will be used. If not '
        'supplied via config, the default log_level for the ingest lib will '
        f'be used: {DEFAULT_LOG_LEVEL_NAME}')
    func = click.option('--log_level', 'log_level_name',
                        type=click.Choice(
                            [level_name.lower()
                             for level_name in logging._nameToLevel.keys()]
                        ),
                        help=log_help_txt)(func)
    # Target URL
    func = click.option(
        '-t', '--target_url',
              default=DEFAULT_TARGET_URL, show_default=True,
              help='Target service URL where data will be loaded into')(func)
    # Stages
    func = click.option(
        '--stages', 'stages_to_run_str',
        default=DEFAULT_STAGES_TO_RUN_STR, show_default=True,
        help=(
            'A string representing the subset of ingest stages that will be '
            'executed by the pipeline. This string can be any '
            f'combination of the characters in '
            f'"{DEFAULT_STAGES_TO_RUN_STR}". Order does not matter, as it is '
            'enforced by the pipeline.'
        ))(func)
    return func


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """
    A CLI utility for ingesting study data into the Kids First ecosystem.

    This method does not need to be implemented. cli is the root group that all
    subcommands will implicitly be part of.
    """
    pass


@click.command()
@click.option('--dry_run',
              default=False,
              is_flag=True,
              help='A flag specifying whether to only pretend to send data to '
              'the target service')
@click.option('--use_async',
              default=False,
              is_flag=True,
              help='A flag specifying whether to use sync or async loading '
              'into the target service')
# ** Temporary - until auto transform is further developed **
# @click.option('--auto_transform',
#               default=False,
#               is_flag=True,
#               help='A flag specifying whether to use auto transformation or '
#               'user guided transformation')
@common_args_options
def ingest(ingest_package_config_path, app_settings_filepath, log_level_name,
           target_url, stages_to_run_str, use_async, dry_run):
    """
    Run the Kids First data ingest pipeline.

    \b
    Arguments:
        \b
        ingest_package_config_path - the path to the data ingest config file
        or a path to a directory which contains a file called
        'ingest_package_config.py'
    """
    # Make kwargs from options
    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    kwargs = {arg: values[arg] for arg in args[1:]}

    # User supplied app settings
    if app_settings_filepath:
        app_settings = settings.load(app_settings_filepath)
    # Default settings
    else:
        app_settings = settings.load()

    kwargs.pop('app_settings_filepath', None)
    kwargs['auth_configs'] = app_settings.AUTH_CONFIGS

    # Run ingest
    pipeline = DataIngestPipeline(
        ingest_package_config_path, app_settings.TARGET_API_CONFIG, **kwargs
    )

    pipeline.logger.info(f'Loaded app settings {app_settings.FILEPATH}, '
                         f'starting in "{app_settings.APP_MODE}" mode')

    perfection = pipeline.run()

    logger = logging.getLogger(__name__)
    if perfection:
        logger.info('✅ Ingest pipline passed validation!')
    else:
        logger.error('❌ Ingest pipeline failed validation! '
                     f'See {pipeline.log_file_path} for details')
        sys.exit(1)


@cli.command()
@common_args_options
@click.pass_context
def test(ctx, ingest_package_config_path, app_settings_filepath,
         log_level_name, target_url, stages_to_run_str):
    """
    Run the Kids First data ingest pipeline in dry_run mode (--dry_run=True)
    Used for testing ingest packages.

    \b
    Arguments:
        \b
        ingest_package_config_path - the path to the data ingest config file
        or a path to a directory which contains a file called
        'ingest_package_config_path.py'
    """
    # Make kwargs from options
    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    kwargs = {arg: values[arg] for arg in args[1:]}

    kwargs['dry_run'] = True
    ctx.invoke(ingest, **kwargs)


@click.command(name='new')
@click.option('--dest_dir',
              help='Path to the directory where the new ingest package will '
              'be created. If not specified, the current working dir will '
              'be used.',
              type=click.Path(exists=False, file_okay=False, dir_okay=True))
def create_new_ingest(dest_dir=None):
    """
    Create a new ingest package based on the template:
    kf_lib_data_ingest.factory.templates.study
    """
    from kf_lib_data_ingest.factory.generate import new_ingest_pkg
    new_ingest_pkg(dest_dir)


cli.add_command(ingest)
cli.add_command(test)
cli.add_command(create_new_ingest)
