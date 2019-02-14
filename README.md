<p align="center">
  <img src="docs/source/_static/images/logo.png">
</p>
<p align="center">
  <a href="https://github.com/kids-first/kf-lib-data-ingest/blob/master/LICENSE"><img src="https://img.shields.io/github/license/kids-first/kf-lib-data-ingest.svg?style=for-the-badge"></a>
  <a href="https://circleci.com/gh/kids-first/kf-lib-data-ingest"><img src="https://img.shields.io/circleci/project/github/kids-first/kf-lib-data-ingest.svg?style=for-the-badge"></a>
  <a href="https://kids-first.github.io/kf-lib-data-ingest"><img src="https://img.shields.io/readthedocs/pip.svg?style=for-the-badge"></a>
</p>

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

### Debug
During the development of a study ingest, a user will likely need
to go through an iterative process of tweaking the extract configs, running
ingest up until the load stage, and inspecting the transformed data which
will be in the form of the standard concept graph. In order to thoroughly debug
or inspect the graph, the developer will need a rich way to visualize and query
the data in the graph.

To accomplish this, the concept graph may be loaded into a Neo4j graph database,
where it can then be visualized and queried through Neo4j's built-in browser
application.

#### Setup Neo4j graph database using Docker
1. To start a new neo4j docker container named `graph-db` do:
```
> docker run --name=graph-db --volume=$HOME/neo4j/data:/data --env=NEO4J_AUTH=none -p 7474:7474 -p 7687:7687 -d neo4j
```

2. On all subsequent start/stops do:
```
docker container start graph-db
docker container stop graph-db
```

#### Load a concept graph into the graph database
Run the neo4j loader utility.

The utility expects a GML file that was written by `etl.transform.standard_model.graph`'s `export_to_gml`
method. As of 10/23/2018, the ingest pipeline does not yet have a clean and isolated way to run the transform stage by itself and optionally export the concept graph to file. For now, a previously generated test concept graph can be used for demonstration purposes.

```
./src/utilities/neo4j_util.py ./test/data/test_graph.gml
```

#### Query and visualize the graph
To visualize/query the graph in the browser application point your browser to
`http://localhost:7474/browser/`

See https://neo4j.com/docs/developer-manual for details on how to use the neo4j browser to query the database using Cypher query language and visualize the graph.

You can also start a Cypher shell in the container to execute queries if you just want a simple interface to the database.
```
> docker exec -it graph-db cypher-shell
```


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

2. Run the docs build server so that docs will auto-reload as edits are made

    ```shell
    sphinx-autobuild docs/source docs/build -p 8000
    ```
The docs site will be available on `localhost:8000`

OR if you really want to, you can still build the docs manually

1. Build the static docs site
    ```
    cd docs
    make html
    ```
This will generate a `build` subdirectory containing the generated docs site.

2. View the docs by opening `docs/build/html/index.html` in the browser
