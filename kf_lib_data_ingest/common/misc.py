import importlib
import inspect
import json
import os
import re
from itertools import tee

from pandas import isnull
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import ConnectionError
import yaml
import jsonpickle

from kf_lib_data_ingest.common.type_safety import assert_safe_type


def import_module_from_file(filepath):
    """
    Import a Python module given a filepath
    """
    module_name = os.path.basename(filepath).split(".")[0]
    spec = importlib.util.spec_from_file_location(module_name, filepath)
    imported_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(imported_module)

    return imported_module


def read_yaml(filepath):
    with open(filepath, 'r') as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.FullLoader)


def read_json(filepath, default=None, use_jsonpickle=True):
    """
    Read JSON file into Python dict. If default is not None and the file
    does not exist, then return default.

    :param filepath: path to JSON file
    :type filepath: str
    :param default: default return value if file not found, defaults to None
    :type default: any, optional
    :param use_jsonpickle: pickle JSON-incompatible types, defaults to True
    :type use_jsonpickle: bool, optional
    :return: your data
    :rtype: dict
    """
    if (default is not None) and (not os.path.isfile(filepath)):
        return default

    with open(filepath, 'r') as json_file:
        if use_jsonpickle:
            json_str = json_file.read()
            return jsonpickle.decode(json_str)
        else:
            return json.load(json_file)


def write_json(data, filepath, use_jsonpickle=True, **kwargs):
    r"""
    Write Python dict to JSON file.

    :param data: your data
    :type data: dict
    :param filepath: where to write your JSON file
    :type filepath: str
    :param use_jsonpickle: pickle JSON-incompatible types, defaults to True
    :type use_jsonpickle: bool, optional
    :param \**kwargs: keyword arguments to pass to json.dump
    """
    if 'indent' not in kwargs:
        kwargs['indent'] = 4
    if 'sort_keys' not in kwargs:
        kwargs['sort_keys'] = True
    with open(filepath, 'w') as json_file:
        if use_jsonpickle:
            data = json.loads(jsonpickle.encode(data))
        json.dump(data, json_file, **kwargs)


def iterate_pairwise(iterable):
    """
    Iterate over an iterable in consecutive pairs
    """
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def numeric_to_str(val, replace_na=False, na=None):
    """
    Converts values to stripped strings while collapsing downcastable floats.

    Examples:
        to_str_with_floats_downcast_to_ints_first(1) -> "1"
        to_str_with_floats_downcast_to_ints_first(1.0) -> "1"
        to_str_with_floats_downcast_to_ints_first("1_1  ") -> "1_1"
        to_str_with_floats_downcast_to_ints_first(None) -> None
        to_str_with_floats_downcast_to_ints_first(None, True, "") -> ""

    If you're wondering what this is good for, try the following:
        import pandas
        df1 = pandas.DataFrame({"a":[1,2,3,None]}, dtype=object)
        df2 = pandas.read_json(df1.to_json(), dtype=object)
        str(df1['a'][0]) == str(df2['a'][0])  # this returns False. Yuck.
        df1 = df1.applymap(to_str_with_floats_downcast_to_ints_first)
        df2 = df2.applymap(to_str_with_floats_downcast_to_ints_first)
        str(df1['a'][0]) == str(df2['a'][0])  # this returns True. Good.

    :param val: any basic type
    :param replace_na: should None/NaN values be replaced with something
    :type replace_na: boolean
    :param na: if replace_na is True, what should None/NaN values be replaced
        with


    :return: new representation of `val`
    """
    if isnull(val):
        if replace_na:
            return na
        else:
            return val

    val = str(val).strip()
    if val != '':
        # Don't automatically change anything with leading zeros, scientific
        # notation, or underscores (I don't care what PEP 515 says).
        if (val[0] == '0') or (not re.fullmatch(r'[\d.]+', val)):
            return val
        try:
            f_val = float(val)
            i_val = int(f_val)
            if i_val == f_val:
                return str(i_val)
        except Exception:
            pass
    return val


def obj_attrs_to_dict(cls):
    """
    Create a dict of obj attributes and values, including inherited attrs
    """
    # Get non function attributes
    attributes = inspect.getmembers(cls, lambda x: not(inspect.isroutine(x)))

    # Get non-hidden attrs
    attributes = [a for a in attributes
                  if not(a[0].startswith('__') and
                         a[0].endswith('__'))]
    return dict(attributes)


def multisplit(string: str, delimiters: list):
    """Split a string by multiple delimiters.

    :param string: the string to split
    :type string: str
    :param delimiters: the list of delimiters to split by
    :type delimiters: list
    :return: the split substrings
    :rtype: list
    """

    assert_safe_type(string, str)
    assert_safe_type(delimiters, list)
    regexPattern = '|'.join(map(re.escape, delimiters))
    return re.split(regexPattern, string)


def upper_camel_case(snake_str):
    """
    Convert a snake case str to upper camel case
    """
    words = snake_str.split('_')
    return ''.join([w.title() for w in words])


def requests_retry_session(
        session=None, total=10, read=10, connect=10, status=10,
        backoff_factor=5, status_forcelist=(500, 502, 503, 504)
):
    """
    Send an http request and retry on failures or redirects

    See urllib3.Retry docs for details on all kwargs except `session`
    Modified source: https://www.peterbe.com/plog/best-practice-with-retries-with-requests # noqa E501

    :param session: the requests.Session to use
    :param total: total retry attempts
    :param read: total retries on read errors
    :param connect: total retries on connection errors
    :param status: total retries on bad status codes defined in
    `status_forcelist`
    :param backoff_factor: affects sleep time between retries
    :param status_forcelist: list of HTTP status codes that force retry
    """
    session = session or requests.Session()

    retry = Retry(
        total=total,
        read=read,
        connect=connect,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        method_whitelist=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session


def get_open_api_v2_schema(url, entity_names,
                           cached_schema_filepath=None, logger=None):
    """
    Get schemas for entities in the target API using {url}/swagger
    endpoint. Will extract parts of the {url}/swagger response to create the
    output dict

    It is expected that swagger endpoint implements the OpenAPI v2.0
    spec. This method currently supports parsing of responses
    with a JSON mime-type.

    See link below for more details on the spec:

    https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md

    Example response from /swagger endpoint:
    {
        'info': {
            'version': '1.9.0',
            'description':  'stuff',
            ...
        },
        'definitions': {
            'Participant': {
                ...
            },
            'Biospecimen': {
                ...
            },
            'BiospecimenResponse': {
                ...
            }
            ...
        }
    }

    Will turn into the output:

    {
        'target_service': https://kf-api-dataservice.kidsfirstdrc.org,
        'version': 1.9.0,
        'definitions': {
            'participant': {
                ...
            },
            'biospecimen': {
                ...
            },
            ...
        }
    }

    Items in `entity_names` must be snake cased versions of existing keys in
    swagger 'definitions'.

    See https://github.com/OAI/OpenAPI-Specification/blob/master/examples/v2.0/json/petstore.json # noqa E501

    :param url: URL to a target service
    :param entity_names: list of snake cased names of entities to extract from
    swagger 'definitions' dict
    :param cached_schema_filepath: file path to a JSON file containing a
    saved version of the target service's schema.
    :param logger: logger to use when reporting errors
    :returns output: a dict with the schema definition and version
    """
    output = None
    err = None
    common_msg = 'Unable to retrieve target schema from target service!'
    schema_url = f'{url}/swagger'

    # Create default cached_schema filepath
    if not cached_schema_filepath:
        cached_schema_filepath = os.path.join(os.getcwd(),
                                              'cached_schema.json')

    # Try to get schemas and version from the target service
    try:
        # ***** TODO remove connect=0, its a temporary hack!!! ***** #
        # Before connect=0, any non-mocked calls to unreachable APIs
        # like dataservice were causing tests to hang. What we really need
        # to do is remove this flag and do integration tests with a
        # live dataservice server - Natasha
        response = requests_retry_session(connect=0).get(schema_url)

    except ConnectionError as e:

        err = f'{common_msg}\nCaused by {str(e)}'

    else:
        if response.status_code == 200:
            # API Version
            version = response.json()['info']['version']
            # Schemas
            defs = response.json()['definitions']
            schemas = {
                k: defs[upper_camel_case(k)]
                for k in entity_names
                if upper_camel_case(k) in defs
            }
            # Make output
            output = {
                'target_service': url,
                'definitions': schemas,
                'version': version
            }
            # Update cache file
            write_json(output, cached_schema_filepath)
        else:
            err = f'{common_msg}\nCaused by unexpected response(s):'
            if response.status_code != 200:
                err += f'\nFrom {schema_url}/swagger:\n{response.text}'

    if err:
        if os.path.isfile(cached_schema_filepath):
            return read_json(cached_schema_filepath)
        else:
            err += ('\nTried loading from cache '
                    f'but could not find file: {cached_schema_filepath}')
        # Report error
        if logger:
            logger.warning(err)
        else:
            print(err)

    return output
