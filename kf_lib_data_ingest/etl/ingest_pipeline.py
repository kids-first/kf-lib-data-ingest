import inspect
import logging
import os
import pprint
from collections import OrderedDict, defaultdict

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.config import DEFAULT_TARGET_URL
from kf_lib_data_ingest.etl.configuration.dataset_ingest_config import (
    DatasetIngestConfig
)
from kf_lib_data_ingest.etl.configuration.log import setup_logger
from kf_lib_data_ingest.etl.extract.extract import ExtractStage
from kf_lib_data_ingest.etl.load.load import LoadStage
from kf_lib_data_ingest.etl.transform.transform import TransformStage

# TODO
# Allow a run argument that contains the desired stages to run
# 'et' or 'tl', etc. If the full pipeline is not specified, then we
# must check for cached stage output


class DataIngestPipeline(object):

    def __init__(self, dataset_ingest_config_path):
        """
        Setup data ingest pipeline. Create the config object and setup logging

        :param dataset_ingest_config_path: Path to config file containing all
        parameters for data ingest.
        """
        self.data_ingest_config = DatasetIngestConfig(
            dataset_ingest_config_path)
        self.ingest_config_dir = os.path.dirname(
            self.data_ingest_config.config_filepath)
        self.ingest_output_dir = os.path.join(self.ingest_config_dir,
                                              'output')

    def run(self, target_api_config_path, auto_transform=False,
            use_async=False, target_url=DEFAULT_TARGET_URL):
        """
        Entry point for data ingestion. Run ingestion in the top level
        exception handler so that exceptions are logged.

        See _run method for param description
        """
        # Create logger
        self._get_log_params(self.data_ingest_config)
        self.logger = logging.getLogger(__name__)

        # Log the start of the run with ingestion parameters
        frame = inspect.currentframe()
        args, _, _, values = inspect.getargvalues(frame)
        param_string = '\n\t'.join(['{} = {}'.format(arg, values[arg])
                                    for arg in args[1:]])
        run_msg = ('BEGIN data ingestion.\n\t-- Ingestion params --\n\t{}'
                   .format(param_string))
        self.logger.info(run_msg)

        # Top level exception handler
        # Catch exception, log it to file and console, and exit
        try:
            self._run(target_api_config_path, auto_transform, use_async,
                      target_url)
        except Exception as e:
            logging.exception(e)
            exit(1)

        # Log the end of the run
        self.logger.info('END data ingestion')

    def _get_log_params(self, data_ingest_config):
        """
        Get log params from data_ingest_config

        :param data_ingest_config a DatasetIngestConfig object containing
        log parameters
        """
        # Get log dir
        log_dir = data_ingest_config.log_dir

        # Get optional log params
        opt_log_params = {param: getattr(data_ingest_config, param)
                          for param in ['overwrite_log', 'log_level']}

        # Setup logger
        setup_logger(log_dir, **opt_log_params)

    def _run(self, target_api_config_path, auto_transform=False,
             use_async=False, target_url=DEFAULT_TARGET_URL):
        """
        Runs the ingest pipeline

        :param target_api_config_path: Path to the target api config file
        :param use_async: Boolean specifies whether to do ingest
        asynchronously or synchronously
        :param target_url: URL of the target API, into which data will be
        loaded. Use default if none is supplied
        """
        # Create an ordered dict of all ingest stages and their parameters
        self.stage_dict = OrderedDict()

        # Extract stage
        self.stage_dict['e'] = (ExtractStage,
                                self.ingest_output_dir,
                                self.data_ingest_config.extract_config_paths)

        # Transform stage
        transform_fp = None
        # Create file path to transform function Python module
        if not auto_transform:
            transform_fp = self.data_ingest_config.transform_function_path
            if transform_fp:
                transform_fp = os.path.join(
                    self.ingest_config_dir, os.path.relpath(transform_fp))

        self.stage_dict['t'] = (TransformStage, target_api_config_path,
                                self.ingest_output_dir, transform_fp)

        # Load stage
        self.stage_dict['l'] = (
            LoadStage, target_api_config_path,
            target_url, use_async,
            self.data_ingest_config.target_service_entities)

        # Iterate over stages and execute them
        output = None
        for key, params in self.stage_dict.items():
            # Instantiate an instance of the ingest stage
            stage = params[0](*(params[1:]))
            # First stage is always extract
            if key == 'e':
                output = stage.run()
                self._post_extract(output)
            else:
                output = stage.run(output)

    def _post_extract(self, extract_output):
        """Performs post-Extract accounting. Here is where we'll perform
        whichever automatic data consistency checks we can do immediately after
        the Extract stage is done. Checks that can only be reliably performed
        after the Transform stage will not happen here.

        :param extract_output: the output returned by ExtractStage.run()
        """

        raw_count = {
            CONCEPT.PARTICIPANT.ID: {
                'name': 'Participants',
                'expected': 'expected_num_participants'
            },
            CONCEPT.BIOSPECIMEN.ID: {
                'name': 'Specimens',
                'expected': 'expected_num_specimens'
            },
            CONCEPT.GENOMIC_FILE.FILE_PATH: {
                'name': 'Genomic Files'
            }
        }

        # THE A_HAS_B CHECKS MIGHT NEED TO GO AFTER TRANSFORM
        a_has_b_checks = {
            "definitely": [
                (CONCEPT.BIOSPECIMEN.ALIQUOT_ID, CONCEPT.PARTICIPANT.ID),
                (CONCEPT.BIOSPECIMEN.ID, CONCEPT.PARTICIPANT.ID)
            ],
            "probably": [
                (CONCEPT.PARTICIPANT.ID, CONCEPT.BIOSPECIMEN.ID),
                (CONCEPT.PARTICIPANT.ID, CONCEPT.BIOSPECIMEN.ALIQUOT_ID)
            ]
        }

        # record the things we want to record

        # counted = {
        #     a_key: {
        #         a1: [file1, file2],
        #         ...
        #     },
        #     ...
        # }
        counted = defaultdict(
            lambda: defaultdict(set)
        )
        # a_has_b = {
        #     (a_key, b_key): {
        #         a1: {
        #             b1: [file1, file2],
        #             ...
        #         },
        #         ...
        #     },
        #     ...
        # }
        a_has_b = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(set)
            )
        )
        # a_missing_b = {
        #     (a_key, b_key): {
        #         a1: [file1, file2],
        #         ...
        #     },
        #     ...
        # }
        a_missing_b = defaultdict(
            lambda: defaultdict(set)
        )

        for config_path, (data_file, df) in extract_output.items():
            # record raw counts
            for key in raw_count.keys():
                if key in df:
                    for val in df[key]:
                        counted[key][val].add(data_file)
            # record A has B checks
            for _, a_b_pairs in a_has_b_checks.items():
                for ab in a_b_pairs:
                    a, b = ab
                    if (a in df) and (b in df):
                        for i, row in df.iterrows():
                            a_has_b[ab][row[a]][row[b]].add(data_file)

        # record A has B check failures
        for _, a_b_pairs in a_has_b_checks.items():
            for ab in a_b_pairs:
                a, b = ab
                for ai, files in counted[a].items():
                    if ai not in a_has_b[ab]:
                        a_missing_b[ab][ai].update(files)

        message = ["POST-EXTRACT ACCOUNTING"]

        # display raw count results

        for k, v in raw_count.items():
            message.append("")
            name = v['name']
            title = name + '  -  \'' + k + '\''
            message.append(title)
            message.append('-' * len(title))
            found = len(counted[k].keys())
            message.append('Found: {}'.format(found))
            if v.get('expected'):
                if v['expected'] in self.data_ingest_config.contents:
                    expected = self.data_ingest_config.contents[v['expected']]
                    message.append(
                        'Expected: {} {}'.format(
                            expected, ('✅' if expected == found else '❌')
                        )
                    )
                else:
                    message.append(
                        'Expected: {}'.format(
                            '\'{}\' not set in {}'.format(
                                v['expected'],
                                self.data_ingest_config.config_filepath
                            )
                        )
                    )

        # display A has B check results

        def _nums_as_nums(val):
            try:
                assert "_" not in val  # I hate PEP 515
                return int(val)
            except ValueError:
                return val

        for check, a_b_pairs in a_has_b_checks.items():
            for ab in a_b_pairs:
                a, b = ab
                message.append("")
                message.append(f"Every {a} should ({check}) have a {b}...")
                if ab in a_missing_b:
                    message.append(f"❌ ...but these don't:")
                    message.append("{ what doesn't : where it is }")
                    message.append(
                        pprint.pformat(
                            {
                                # pprint forcibly sorts keys, so we trick it
                                # for any key that looks like it could be a
                                # stringified int
                                _nums_as_nums(key): a_missing_b[ab][key]
                                for key in a_missing_b[ab].keys()
                            }
                        )
                    )
                else:
                    message.append("✅")

        message.append("")
        self.logger.info("\n".join(message))
