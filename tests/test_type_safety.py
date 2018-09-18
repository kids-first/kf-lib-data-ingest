"""
Tests for src/common/safety.py
"""

import pandas
import pytest

from common.type_safety import _name_of_arg_at_caller, function, type_assert


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
    ((5, 5), tuple),
    (int, callable)
]


def test_unwitting_user():
    type_assert(5, int)  # Why are they asking?!
    with pytest.raises(ValueError):
        type_assert(5, float)  # Why are they asking?!


# Special care is needed for the tests to cover the fact that functions are
# callable but not vice versa.
def _func_call(v, w):
    return (v is function) and (w is callable)


def test_single_good_single_bad_type_checking():
    """
    Test all combinations of values and types in type_exemplars and make sure
    that TypeError is raised if the val is not matched with its approved type.
    """
    for k, v in type_exemplars:
        for _, w in type_exemplars:
            if (v is w) or _func_call(v, w):
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
            if (v is not w) and not _func_call(v, w):
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
    """
    type_assert(lambda x: x, callable)
    type_assert(lambda x: x, function)

    type_assert(int, callable)

    with pytest.raises(TypeError):
        type_assert(int, function)


def test_name_in_error_message():
    pickle = 5
    pockle = 5
    with pytest.raises(TypeError):
        try:
            type_assert(pickle, bool)
        except Exception as e:
            assert 'pickle' in e
            raise

    with pytest.raises(TypeError):
        try:
            type_assert(pockle, bool)
        except Exception as e:
            assert 'pockle' in e
            raise


# test checking outside of a function (yes this matters)
foo = 5
type_assert(foo, int)
with pytest.raises(TypeError):
    type_assert(foo, float)
