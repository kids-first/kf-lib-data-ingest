import inspect


def function(x):
    """
    Lets you use `function` as a type in type_check and type_assert.
    """
    return inspect.isfunction(x) or inspect.isbuiltin(x)


_basic_types = {int, float, bool, str, bytes}


def type_check(val, *safe_types):
    """
    Check if val is one of the declared safe types.
    Calling Example:
        type_check(my_val, int, float)  # checks if my_val is an int or float

    :param val: some value
    :param *args: type classes or truthy-returning functions
                  e.g. int, str, callable
    :returns: True/False depending on whether val is a safe type
    """
    types = []
    val_type = type(val)
    for t in safe_types:
        if t in {function, callable}:
            if t(val):
                return True
        elif t in _basic_types:
            if val_type is t:
                return True
        else:
            if t is None:
                t = type(None)
            types.append(t)
    return isinstance(val, tuple(types))


def type_assert(name, val, *safe_types):
    """
    Raise an exception of val is not one of the declared safe types.
    Calling Example:
        type_assert(my_func, function)  # my_func must be a function
        type_assert(my_val, int, float)  # my_val must be int or float

    :raises: TypeError if type_check(val, safe_types) returns False
    """
    assert safe_types
    if not type_check(val, *safe_types):
        stack = inspect.stack()
        caller = stack[1]
        type_names = tuple(
            [t.__name__ if t is not None else "None" for t in safe_types]
        )
        # TODO: look into getting the variable name automatically
        # We shouldn't have to pass it in. It has to be available in the
        # call stack.
        raise TypeError(
            "{}:{} requires {} to be one of {}"
            .format(caller.filename, caller.function, name, type_names)
        )
