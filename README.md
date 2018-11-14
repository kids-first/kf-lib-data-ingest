<p align="center"
  <img src="docs/img/kf-data-ingest.png"
</p
<p align="center"
  <a href="https://github.com/kids-first/kf-lib-data-ingest/blob/master/LICENSE"<img src="https://img.shields.io/github/license/kids-first/kf-lib-data-ingest.svg?style=for-the-badge"</a
  <a href="https://circleci.com/gh/kids-first/kf-lib-data-ingest"<img src="https://img.shields.io/circleci/project/github/kids-first/kf-lib-data-ingest.svg?style=for-the-badge"</a
  <a href="https://kids-first.github.io/kf-lib-data-ingest"<img src="https://img.shields.io/readthedocs/pip.svg?style=for-the-badge"</a
</p

Kids First Data Ingest Library
================================

The Kids First Data Ingest Library includes both an ingestion library and CLI. The library is composed of a set of components that standardize the transformation and ingestion of datasets into target services. A CLI is provided for users of the ingestion pipeline. Users will run ingestions for Kids First studies into the Kids First Dataservice, which is the only supported target service at this point.

## Documentation
The latest documentation can be found at:
https://kids-first.github.io/kf-lib-data-ingest/


## Getting Started - Users
As a user, it is expected that you will primarily run ingestion and optionally make modifications to configuration through the dataset ingest configuration file. If you want to change how source data is transformed, visit the [Getting Started - Developers](#getting-started-developers)

### Install
```
pip install -e git+https://github.com/kids-first/kf-lib-data-ingest.git#egg=kf-lib-data-ingest
```

### Run
```
kidsfirst ingest /path/to/dataset_ingest_config.yml
```
Run kidsfirst -h to see all commands and descriptions.

## Getting Started - Developers

### Setup dev environment
1. Git clone the repository
```
git clone git@github.com:kids-first/kf-lib-data-ingest.git
```
2. Setup and activate virtual env
```
python3 -m venv venv
source venv/bin/activate
```
3. Install kf-lib-data-ingest along with dependencies
```
pip install -e .
```
4. Install dev requirements
```
pip install -r dev-requirements.txt
```

### Run unit tests
```
python -m pytest tests
```

### Build documentation
We currently use Sphinx for generating the API/reference documentation for the ingest library.

If you would like to develop documentation follow these steps:
1. Install the Python packages needed to build the documentation

```
pip install -r doc-requirements.txt
```
2. Build the static docs site

After making changes to docstrings in the source code or .rst files in docs/source, you will need to rebuild the html:
```
cd docs
make html
```

3. This will generate a `build` subdirectory containing the generated docs site. View the docs by opening `docs/build/html/index.html` in the browser
