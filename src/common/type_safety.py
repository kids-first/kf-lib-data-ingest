import ast
import inspect


__UNNAMED_OBJECT = 'unnamed object of type'
__ALL_SIGNIFIER = 'all items in '


# This function supports finding the _name_ of the first argument passed to
# assert_safe_type.
def _ast_object_name(obj):
    """
    If obj has a name, returns it.
    If obj is a call, recurse into it and note the call chain (with arguments).
    Otherwise raises ValueError indicating that the object has no identifier.
    """
    if not isinstance(obj, ast.Name):
        if isinstance(obj, ast.Attribute):
            return _ast_object_name(obj.value) + '.' + obj.attr
        elif isinstance(obj, ast.Call):
            call = _ast_object_name(obj.func)
            arguments = []
            if hasattr(obj, 'keywords'):
                arguments = [
                    str(ast.literal_eval(a)) for a in obj.args
                ] + [
                    kob.arg + '=' + str(ast.literal_eval(kob.value))
                    for kob in obj.keywords
                ]
            return call + '(' + ', '.join(arguments) + ')'
        else:
            # unnamed values (5, 'foo', etc.) obviously have no name
            raise ValueError(
                __UNNAMED_OBJECT + ' <{}>'.format(type(obj).__name__)
            )
    return obj.id


# This function supports finding the _name_ of the first argument passed to
# assert_safe_type.
def _varname_from_ast_node(ast_node, arg_name, arg_index):
    """
    Find the calling argument name from the ast node that has the call. If the
    argument was not a variable, raises a ValueError declaring that the object
    is unnamed.
    """
    calling_arg_name = None

    # look at keywords first in case the caller did something funny
    for kwarg in ast_node.keywords:
        if kwarg.arg == arg_name:
            calling_arg_name = _ast_object_name(kwarg.value)

    # look at first arg now that we know it's not weirdly put into kwargs
    if (calling_arg_name is None) and (len(ast_node.args) > arg_index):
        calling_arg_name = _ast_object_name(ast_node.args[arg_index])

    return calling_arg_name


# This function supports finding the _name_ of the first argument passed to
# assert_safe_type.
def _name_of_arg_at_caller(which_arg=0, frames_higher=1):
    """
    Returns the _name_ of the variable that was passed in as the nth argument
    to the function that called this.

    Find where the requesting function is called and see what variable _name_
    was passed in for its nth argument when _it_ was called. If the nth
    argument was not a variable, raises a ValueError.

    This seems to be pretty tricky to do so I've commented verbosely.

    example:
        def my_test_func(a, b, c):
            return _name_of_arg_at_caller(1)

        a_var, b_var, c_var = 1, 2, 3
        my_test_func(a_var, b_var, c_var)  # should return the string "b_var"
    """
    # When you pass an argument to a function, only the value goes through to
    # the next stack frame, not the name of the variable that contained the
    # value. But we want the name of the variable, so lets try to recover it by
    # reconstructing the code from the line where the call was made.

    # Build the AST for the source file that contains the call in question
    # (this parent's parent) and then find the AST node where the call occurred
    # and then look at its calling arguments.

    # stack frame of this function
    this_frame = inspect.currentframe()

    # frame of the function that's asking (parent frame of this)
    asking_frame = inspect.getouterframes(this_frame)[frames_higher].frame
    asking_name = asking_frame.f_code.co_name

    # name of the asking function's nth argument as seen from the inside
    # (we want to get to it from the outside)
    which_arg_name = list(asking_frame.f_locals)[which_arg]

    # frame where the asking function was called (parent of the parent frame)
    # (this is where the key function call occurs)
    calling_frame = inspect.getouterframes(asking_frame)[1]

    # The line number specified for the call is equal to or greater than the
    # line where the function call begins (this handles function calls that
    # span multiple lines), so find the last AST node whose lineno is not
    # greater than the lineno in the calling frame.

    # line where the function call begins
    call_line = inspect.getframeinfo(calling_frame[0]).lineno

    # inspect.findsource will raise OSError('could not get source code') if
    # you're trying to call _name_of_arg_at_caller from a bare interactive
    # shell with no code file. But if you're in a bare interactive shell with
    # no code file then you should already know the answer and shouldn't be
    # calling this function!
    file_src = inspect.findsource(calling_frame[0])[0]
    ast_nodes = ast.parse(''.join(file_src))

    # finding the right AST node
    call_node = None
    all_call_nodes = iter(
        n for n in ast.walk(ast_nodes) if isinstance(n, ast.Call)
    )
    for node in sorted(all_call_nodes, key=lambda n: n.lineno):
        if node.lineno > call_line:
            break
        func = node.func
        if hasattr(func, 'id') and func.id == asking_name:
            call_node = node

    # find the argument name from the node that has the call
    return _varname_from_ast_node(call_node, which_arg_name, which_arg)


def function(x):
    """
    Lets you use `function` as a type in safe_type_check and assert_safe_type.
    """
    return inspect.isfunction(x) or inspect.isbuiltin(x)


_basic_types = {int, float, bool, str, bytes}


def safe_type_check(val, *safe_types):
    """
    Check if val is one of the declared safe types.
    Calling Example:
        # checks if my_val stores an int or float
        safe_type_check(my_val, int, float)

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


def _raise_error(safe_types, is_container=False):
    caller = inspect.stack()[2]
    try:
        name = _name_of_arg_at_caller(0, 2)
    except ValueError as e:
        name = str(e)

    type_names = [
        t.__name__ if hasattr(t, '__name__') else t for t in safe_types
    ]

    if is_container:
        name = __ALL_SIGNIFIER + name
    raise TypeError(
        '{}:{}:{} requires {} to be one of these types: {}'
        .format(caller.filename, caller.lineno, caller.function, name,
                type_names)
    )


def assert_safe_type(val, *safe_types):
    """
    Raise an exception if val is not one of the declared safe types.
    Calling Example:
        assert_safe_type(my_func, function)  # my_func must be a function
        assert_safe_type(my_val, int, float)  # my_val must be int or float

    :raises: TypeError if safe_type_check(val, safe_types) returns False
    """
    assert safe_types
    if not safe_type_check(val, *safe_types):
        _raise_error(safe_types)


def assert_all_safe_type(val_list, *safe_types):
    """
    Raise an exception if any of the members of val_list are not one of the
    declared safe types.
    Calling Example:
        # my_dict must have only str keys
        assert_all_safe_type(my_dict.keys(), str)

    :raises: TypeError if safe_type_check(val, safe_types) returns False for
        any of the members.
    """
    assert safe_types
    for val in val_list:
        if not safe_type_check(val, *safe_types):
            _raise_error(safe_types, True)
