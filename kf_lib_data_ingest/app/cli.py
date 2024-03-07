"""
Entry point for the Kids First Data Ingest Client
"""

import inspect
import logging
import os
import sys

import click

from kf_lib_data_ingest.app import settings
from kf_lib_data_ingest.config import DEFAULT_LOG_LEVEL, DEFAULT_TARGET_URL
from kf_lib_data_ingest.common.stage import (
    BASIC_VALIDATION,
    ADVANCED_VALIDATION,
)
from kf_lib_data_ingest.etl.ingest_pipeline import (
    DEFAULT_STAGES_TO_RUN_STR,
    VALID_STAGES_TO_RUN_STRS,
    DataIngestPipeline,
)

CONTEXT_SETTINGS = {
    "help_option_names": ["-h", "--help"],
    "max_content_width": 9999,  # workaround for https://github.com/pallets/click/issues/486
}
DEFAULT_LOG_LEVEL_NAME = logging._levelToName.get(DEFAULT_LOG_LEVEL)
DEFAULT_VALIDATION_MODE = ADVANCED_VALIDATION
VALIDATION_MODE_OPT = {
    "args": ("--validation_mode",),
    "kwargs": {
        "default": DEFAULT_VALIDATION_MODE,
        "type": click.Choice([BASIC_VALIDATION, ADVANCED_VALIDATION]),
        "help": (
            "Does not apply if --no_validate CLI flag is present. "
            f"The `{BASIC_VALIDATION}` mode runs validation faster but is not "
            f"as thorough. The {ADVANCED_VALIDATION} mode takes into account "
            "implied relationships in the data and is able to resolve "
            "ambiguities or report the ambiguities if they cannot be resolved."
            "\nFor example, you have a file that relates participants and "
            "specimens, and a file that relates participants and genomic files."
            "This means your specimens have implied connections to their "
            f"genomic files through the participants. In {ADVANCED_VALIDATION}"
            "mode, the validator may be able to resolve these implied "
            f"connections and report that all specimens are validly linked to "
            f"genomic files. In {BASIC_VALIDATION} mode, the validator will "
            "report that all specimens are missing links to genomic files."
        ),
    },
}


def common_args_options(func):
    """
    Common click args and options
    """
    func = click.argument(  # Path to the data ingest package directory
        "ingest_package_path",
        type=click.Path(exists=True, file_okay=True, dir_okay=True),
    )(func)

    func = click.option(
        "-q",
        "--query_url",
        default="",
        help=(
            "Service URL that unique identifiers will be queried from during"
            " loading if not in the local cache (defaults to the load"
            " target URL)."
        ),
    )(func)

    # Clear the UID cache
    func = click.option(
        "--clear_cache",
        default=False,
        is_flag=True,
        help=(
            "Clear the local identifier cache before loading into the target"
            " service. This should always be safe unless it's the only location"
            " where the target identifiers are stored (i.e. they've been"
            " temporarily erased from the target service)."
        ),
    )(func)

    # Resume loading from
    func = click.option(
        "--resume_from",
        default=None,
        help=("Dry run until a given target ID, and then load starting there."),
    )(func)

    # Multithreaded loading
    func = click.option(
        "--use_async",
        default=False,
        is_flag=True,
        help=(
            "Load into the target service using multiple asynchronous requests"
            " at a time instead of only one request at a time."
            " This is potentially much faster, but consequently puts much more"
            " strain on the target service."
        ),
    )(func)

    # Stages
    func = click.option(
        "--stages",
        "stages_to_run_str",
        default=DEFAULT_STAGES_TO_RUN_STR,
        show_default=True,
        type=click.Choice(VALID_STAGES_TO_RUN_STRS),
        help=(
            "A string representing the subset of ingest stages that will be "
            "executed by the pipeline. Each char in the string represents a "
            "stage in the pipeline. Chars must follow the order of stage "
            "execution set by the pipeline and contain no gaps in the "
            "stage sequence."
        ),
    )(func)

    # Target URL
    func = click.option(
        "-t",
        "--target_url",
        default=DEFAULT_TARGET_URL,
        show_default=True,
        help="Target service URL where data will be loaded into",
    )(func)

    # App settings
    func = click.option(
        "--app_settings",
        "app_settings_filepath",
        type=click.Path(exists=True, file_okay=True, dir_okay=False),
        help=(
            "Path to an ingest app settings file. If not specified, "
            "will use default app settings for the current app mode, "
            "which is specified by environment variable: "
            f"{settings.APP_MODE_ENV_VAR}. "
            "See kf_lib_data_ingest.app.settings for default settings "
            "files."
        ),
    )(func)

    # Log level
    log_help_txt = (
        "Controls level of log messages to output. If not supplied via CLI, "
        "log_level from the ingest_package_config.py will be used. If not "
        "supplied via config, the default log_level for the ingest lib "
        f"({DEFAULT_LOG_LEVEL_NAME}) will be used."
    )
    func = click.option(
        "--log_level",
        "log_level_name",
        type=click.Choice(
            [level_name.lower() for level_name in logging._nameToLevel.keys()]
        ),
        help=log_help_txt,
    )(func)

    # Disable data validation
    func = click.option(
        "--no_validate",
        default=False,
        is_flag=True,
        help="Skip the data validation step.",
    )(func)

    # Validation mode
    func = click.option(
        *VALIDATION_MODE_OPT["args"], **VALIDATION_MODE_OPT["kwargs"]
    )(func)

    return func


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
def cli():
    """
    A CLI utility for ingesting tabular data into the Kids First ecosystem.

    This method does not need to be implemented. cli is the root group that all
    subcommands will implicitly be part of.
    """
    pass


@click.command()
@common_args_options
@click.option(
    "--dry_run",
    default=False,
    is_flag=True,
    help="A flag specifying whether to only pretend to send data to "
    "the target service. Overrides the resume_from setting.",
)
@click.option(
    "--warehouse",
    default=False,
    is_flag=True,
    help="Send data to the warehouse db.",
)
def ingest(
    ingest_package_path,
    app_settings_filepath,
    log_level_name,
    target_url,
    stages_to_run_str,
    use_async,
    dry_run,
    resume_from,
    warehouse,
    no_validate,
    validation_mode,
    clear_cache,
    query_url,
):
    """
    Run the Kids First data ingest pipeline.

    \b
    Arguments:
        \b
        INGEST_PACKAGE_PATH - Path to the data ingest package directory
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

    if not kwargs.pop("warehouse"):
        os.environ[app_settings.SECRETS.WAREHOUSE_DB_URL] = ""

    if kwargs.pop("no_validate"):
        kwargs["validation_mode"] = None

    kwargs.pop("app_settings_filepath", None)
    kwargs["auth_configs"] = app_settings.AUTH_CONFIGS
    kwargs["db_url_env_key"] = app_settings.SECRETS.WAREHOUSE_DB_URL

    # Run ingest
    pipeline = DataIngestPipeline(
        ingest_package_path, app_settings.TARGET_API_CONFIG, **kwargs
    )

    pipeline.logger.info(
        f"Loaded app settings {app_settings.FILEPATH}, "
        f'starting in "{app_settings.APP_MODE}" mode'
    )

    pipeline.run()


@cli.command()
@common_args_options
@click.pass_context
def test(
    ctx,
    ingest_package_path,
    app_settings_filepath,
    log_level_name,
    target_url,
    stages_to_run_str,
    use_async,
    resume_from,
    no_validate,
    validation_mode,
    clear_cache,
    query_url,
):
    """
    Run the Kids First data ingest pipeline with the --dry_run
    flag active. Used for testing ingest packages.

    \b
    Arguments:
        \b
        INGEST_PACKAGE_PATH - Path to the data ingest package directory
    """
    # Make kwargs from options
    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    kwargs = {arg: values[arg] for arg in args[1:]}

    kwargs["dry_run"] = True
    ctx.invoke(ingest, **kwargs)


@click.command(name="new")
@click.option(
    "--dest_dir",
    help="Path to the directory where the new ingest package will "
    "be created. If not specified, the current working dir will "
    "be used.",
    type=click.Path(exists=False, file_okay=False, dir_okay=True),
)
def create_new_ingest(dest_dir=None):
    """
    Create a new ingest package based on the
    `kf_lib_data_ingest.templates.my_ingest_package` template.
    """
    from kf_lib_data_ingest.factory.generate import new_ingest_pkg

    new_ingest_pkg(dest_dir)


@click.command()
@click.argument(
    "file_or_dir",
    type=click.Path(exists=True, file_okay=True, dir_okay=True),
)
@click.option(*VALIDATION_MODE_OPT["args"], **VALIDATION_MODE_OPT["kwargs"])
def validate(file_or_dir, validation_mode=DEFAULT_VALIDATION_MODE):
    """
    Validate files and write validation reports to
    a subdirectory, `validation_results`, in the current working directory.

    \b
    Arguments:
        \b
        FILE_OR_DIR - the path to the file or directory of files to validate
    """
    from kf_lib_data_ingest.common.io import path_to_file_list
    from kf_lib_data_ingest.validation.validation import Validator

    success = False
    v = Validator(
        output_dir=os.path.abspath(
            os.path.join(os.path.dirname(file_or_dir), "validation_results")
        )
    )
    try:
        if validation_mode == BASIC_VALIDATION:
            include_implicit = False
        else:
            include_implicit = True

        success = v.validate(
            path_to_file_list(file_or_dir), include_implicit=include_implicit
        )
    except Exception as e:
        v.logger.exception(str(e))

    if success:
        v.logger.info("✅ Data validation passed!")
    else:
        v.logger.error("❌ Data validation failed!")
        sys.exit(1)


cli.add_command(ingest)
cli.add_command(test)
cli.add_command(create_new_ingest)
cli.add_command(validate)
