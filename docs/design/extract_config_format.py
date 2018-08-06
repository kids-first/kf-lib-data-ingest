# IMPORT CONSTANTS
# We don't want replicated strings in your config. If it's being compared to
# something anywhere else, it should come from the constants file.
from common import constants


# REQUIRED: source data file url declaration
source_data_url = '<protocol>://<path>'
# Supported protocols are currently: s3, file, http, https
# See also: src/common/file_retriever.py:FileRetriever._getters

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
operations = [
    {
        "out": <standard concept property string for resulting column>,
        "in": <column header or numeric index of column in source file>,
        "val_mapper": <see note on val_mapper args>
    },
    {
        "out": <standard concept property string for resulting column>,
        "row_mapper": <function that takes an entire row and returns a value>
    },
    {
        "out": <standard concept property string for resulting column>,
        "df_mapper": <function that takes an entire df and returns a column>
    },
    # ...
]

# We can define other operation functions, such as melt, and apply them
# similarly as val_mapper, row_mapper, and df_mapper above, and they can have
# additional argument fields defined as needed.

# NOTE on df_mapper
#
# If we decide that df_mapper should be able to return multiple columns and not
# just one, then melt becomes a special case of that.

# NOTE on val_mapper args
#
# "val_mapper" can be a function, a constant, a dict, or a list of dicts.
#
# If a function, it will be applied to every value in the source column to
#     produce values for the output column.
# If a constant, every value in the output column will be set to that constant.
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
#     Python 3.7+ doesn't need this.


# OPERATIONS NOTE
#
# multiple instances of the same "out" column should produce vertical
# duplication of all other columns in the file, to be used for parallel columns
# that map to the same concept, such as in this example below.
#
# The most desirable outcome for
#
# specimen | cram_files | bam_files
# 1        | "1.cram" | "1.bam"
# 2        | "2.cram" | "2.bam"
#
# would be to generate
#
# specimen | genomic_files
# 1        | "1.cram"
# 2        | "2.cram"
# 1        | "1.bam"
# 2        | "2.bam"
#
# which we can do by re-iterating the specimen column (and others) when adding
# the second set of genomic files


# OPERATIONS NOTE
#
# Alternative form for the outer operation dicts
#
# instead of this
# [
#     {
#         "out": foo,
#         "in": bar,
#         "val_mapper": qux
#     },
#     ...
# ]
#
# we can also very easily support this format
# [
#     (val_mapper, foo, bar, qux),
#     ...
# ]
#
# The dict form is more self-explanatory, but it's also more verbose. We can
# easily check if the container is a tuple or dict, handle one type, and
# convert the other type into the handled type.