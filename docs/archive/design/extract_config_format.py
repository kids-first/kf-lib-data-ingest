# IMPORT CONSTANTS
# We don't want replicated strings in your config. If it's being compared to
# something anywhere else, it should come from the constants file.
from common import constants
from etl.extract.operations import *


# REQUIRED: source data file url declaration
# Only _one_ data file per config file. File merging happens in another stage.
# Supported protocols are currently: s3, file, http, https
# See also: src/common/file_retriever.py:FileRetriever._getters
source_data_url = '<protocol>://<path>'


# REQUIRED: source data loading arguments
source_data_loading_parameters = {
    "load_function": <function pointer>,
    <argument name>: <value>,
    # etc...
}

# Since it's a python file, you can define your own local functions as needed.
# Just don't break the required fields.
def some_function(x):
    return x

# REQUIRED (probably): operations for pulling columns out of the file data
# The goal is to generate a new dataframe that represents the associated file
# data using standard concepts and values.
operations = [
    value_map(
        m= # see note below on value_map.m
        out_col= # standard concept property for resulting column
        in_col= # column header or numeric index of column in source file
    ),
    # column_map is just like value_map but different
    column_map(
        m= # function that takes a column and returns a column
        out_col= # standard concept property for resulting column
        in_col= # column header or numeric index of column in source file
    ),
    row_map(
        m= # function that takes a row and returns a value
        out_col= # standard concept property for resulting column
    ),
    constant_map(
        m= # a constant value
        out_col= # standard concept property for resulting column
    ),
    melt_map(  # combines pandas.melt (see pandas docs) with value_map
        var_name= # standard concept property for resulting var_name column
        var_map= # dict of file_column:standard_name replacements for melt vars
        value_name= # standard concept property for resulting value_name column
        value_map= # value_map.m structure for the melt values
    ),
    # df_map is the mother of all maps
    df_map(
        m= # function that takes a df and returns a standard concepts df
    ),
    # ...
    #
    # The code will also support sub-lists of operations for when certain
    # extracted columns need to be logically grouped together (e.g. phenotype
    # name + age at observation), though it should, in the end, be functionally
    # equivalent to spinning out a new extract config file for each logical
    # group.
]

# NOTE on value_map.m
#
# value_map.m can be a function, a dict, or a list of dicts.
#
# If a function, it will be applied to every value in the source column to
#     produce values for the output column.
# If a dict, it will be applied using a function that, for each dict key, finds
#     cells in the source column that match that key when interpreted as an
#     implicitly ^$ anchored regular expression from the set of cells that have
#     not already been matched by previous dict keys, and then, if the dict
#     value is a constant, replaces the original value with the new constant
#     value, or, if the dict value is a function, calls that function passing
#     in any captures present in the regex pattern or the whole cell value if
#     no captures are specified, and then replaces the original value with the
#     function's returned value.
# If a list of dicts, it does the same as for dicts, but in ordered batches.
#     CPython 3.6+ doesn't need this. PythonLang 3.7+ doesn't need this.


# OPERATIONS NOTE
#
# multiple instances of the same "out" column will produce stacked output of
# the multiple results for that column and vertical multiplication of the other
# columns in the file, to be used in cases of parallel columns that map to the
# same concept, such as in this example below.
#
# The most desirable outcome for
#
# specimen | cram_files | bam_files
# 1        | "1.cram"   | "1.bam"
# 2        | "2.cram"   | "2.bam"
#
# would be to generate
#
# specimen | genomic_files
# 1        | "1.cram"
# 2        | "2.cram"
# 1        | "1.bam"
# 2        | "2.bam"
#
# which we can do by re-iterating the specimen column when adding the second
# set of genomic files


# NOTE on alternative forms for the operations sequence
#
# We currently only support function wrappers and lists for operations, but...
#
# Instead of directly calling the function wrappers during config load, like
#
# [
#     value_map(
#         m=foo,
#         out_col=bar,
#         in_col=qux
#     ),
#     ...
# ]
#
# we could also very easily support other formats of varying descriptiveness
# and compactness, like
#
# [
#     (value_map, foo, bar, qux),
#     ...
# ]
#
# or
#
# [
#     {
#         "out": bar
#         "in": qux
#         "val_map": foo
#     },
#     ...
# ]
#
# We can easily check if the container is a tuple or dict, and then convert the
# alternate type into the wrapper function call.
