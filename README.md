Kids First Data Ingest Libraries
==============================

The Kids First Data Ingest Libraries include both an ingestion library and CLI. The library is composed of a set of components that standardize the transformation and ingestion of datasets into target services. A CLI is provided for users of the ingestion pipeline. Users will run ingestions for Kids First studies into the Kids First Dataservice, which is the only supported target service at this point.

## Getting Started - Users
As a user, it is expected that you will primarily run ingestion and optionally make modifications to configuration through the dataset ingest configuration file. If you want to change how source data is transformed, visit the Getting Started - Developers section.

TBD

## Getting Started - Developers

### Setup
1. Git clone the repository
```
> git clone git@github.com:kids-first/kf-lib-data-ingest.git
```
2. Setup and activate virtual env
```
> virtualenv -p python3 venv
> source venv/bin/activate
```
3. Install run and dev requirements
```
> pip install -r requirements.txt
> pip install -r dev-requirements.txt
```
4. Install kf-lib-data-ingest
```
> pip install .
```

### Run
```
> ./src/main.py /path/to/dataset_ingest_config.yml
```
Run ./src/main.py -h to see all command line argument descriptions.

### Test
```
> python -m pytest tests
```
