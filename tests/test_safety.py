"""
Tests for src/common/safety.py
"""

import pandas
import pytest
from common.safety import type_assert, function

# values and the type of value that they are
type_exemplars = [
    (5, int),
    (5.1, float),
    ("5", str),
    (b"5", bytes),
    (lambda x: x, function),
    (True, bool),
    (False, bool),
    (0, int),
    (1, int),
    (0.0, float),
    (1.0, float),
    (None, None),
    (pandas.DataFrame(), pandas.DataFrame),
    (pandas.Series(), pandas.Series),
    ({5: 5}, dict),
    ([5, 5], list),
    ({5, 5}, set),
    ((5, 5), tuple)
]


def test_single_good_single_bad_type_checking():
    """
    Test all combinations of values and types in type_exemplars and make sure
    that TypeError is raised if the val is not matched with its approved type.
    """
    # single good / single bad checks
    for k, v in type_exemplars:
        for _, w in type_exemplars:
            if v is w:
                type_assert(str(k), k, v)
            else:
                with pytest.raises(TypeError):
                    type_assert(str(k), k, w)


def test_complex_bad_type_checking():
    """
    Test each value in type_exemplars against all types except its own and make
    sure that TypeError is raised each time.
    """
    for k, v in type_exemplars:
        to_compare = []
        for l, w in type_exemplars:
            if v is not w:
                to_compare.append(w)
        with pytest.raises(TypeError):
            type_assert(str(k), k, *to_compare)


def test_complex_good_type_checking():
    """
    Test each value in type_exemplars against all the types and make sure that
    TypeError is not raised.
    """
    for k, v in type_exemplars:
        to_compare = []
        for l, w in type_exemplars:
            to_compare.append(w)
        type_assert(str(k), k, *to_compare)
