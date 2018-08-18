"""
Tests for src/common/safety.py
"""

import pandas
import pytest
from common.safety import type_assert, function, _name_of_arg_at_caller


def test__name_of_arg_at_caller():
    def test_func(a):
        return _name_of_arg_at_caller(0)

    with pytest.raises(ValueError):
        test_func(1)

    with pytest.raises(ValueError):
        test_func(lambda x: x)

    with pytest.raises(ValueError):
        test_func([])

    test_var = 5
    assert test_func(test_var) == 'test_var'


# Values and the type of value that they are.
# Don't put something with unidirectional overlap in here, because it will
# require changing the test functions.
#
# (int, callable) is one example, since anything that matches `function` will
# also match `callable`, which is not what this table is intended to test.
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
    ({5, 6}, set),
    ((5, 5), tuple)
]


def test_single_good_single_bad_type_checking():
    """
    Test all combinations of values and types in type_exemplars and make sure
    that TypeError is raised if the val is not matched with its approved type.
    """
    for k, v in type_exemplars:
        for _, w in type_exemplars:
            if v is w:
                type_assert(k, v)
            else:
                with pytest.raises(TypeError):
                    type_assert(k, w)


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
            type_assert(k, *to_compare)


def test_complex_good_type_checking():
    """
    Test each value in type_exemplars against all the types and make sure that
    TypeError is not raised.
    """
    for k, v in type_exemplars:
        to_compare = []
        for l, w in type_exemplars:
            to_compare.append(w)
        type_assert(k, *to_compare)


def test_callable_type_checking():
    """
    Test functionality of `function` vs `callable` arguments.

    Don't put something like (int, callable) into the type_exemplars list,
    because functions are also callable and then handling that makes the other
    test functions more ugly.
    """
    type_assert(int, callable)

    with pytest.raises(TypeError):
        type_assert(int, function)

    with pytest.raises(TypeError):
        type_assert(int, int)
