import os

from etl.ingest_pipeline import DataIngestPipeline


def main():
    TEST_ROOT_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    TEST_ROOT_DIR = os.path.join(TEST_ROOT_DIR, 'tests')
    TEST_DATA_DIR = os.path.join(TEST_ROOT_DIR, 'data')

    target_api_config_path = os.path.join(TEST_DATA_DIR,
                                          'test_api_config.py')
    data_ingest_config_path = os.path.join(TEST_DATA_DIR,
                                           'test_study',
                                           'data_ingest_config.yml')
    p = DataIngestPipeline(data_ingest_config_path)

    # Update data ingest config
    p.data_ingest_config.contents['logging'] = {
        'log_level': 'info'}
    p.data_ingest_config._set_log_params()

    # Run and generate logs
    p.run(target_api_config_path)


if __name__ == '__main__':
    main()
