import ast
import inspect


def _ast_object_name(obj):
    """
    Checks if obj has a name and returns it, and otherwise raises ValueError.
    """
    if not isinstance(obj, ast.Name):
        # unnamed values (5, "foo", etc.) obviously have no name
        raise ValueError(
            'object of type <{}> is not named'.format(type(obj).__name__)
        )
    return obj.id


def _varname_from_ast_node(ast_node, arg_name, arg_index):
    """
    Find the calling argument name from the ast node that has the call. If the
    argument was not a variable, raises a ValueError.
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


def _name_of_arg_at_caller(which_arg=0):
    """
    Find where the requesting function is called and see what variable name was
    passed in for its nth argument. If the nth argument was not a variable,
    raises a ValueError.
    """
    # stack frame of this function
    this_frame = inspect.currentframe()

    # frame of the function that wants to know
    asking_frame = inspect.getouterframes(this_frame)[1].frame
    asking_name = asking_frame.f_code.co_name

    # name of the asking function's nth argument
    which_arg_name = list(asking_frame.f_locals)[which_arg]

    # frame where the function that wants to know was called
    calling_frame = inspect.getouterframes(asking_frame)[1]

    # Find the AST node where the call occured and then look at its calling
    # arguments.

    # First build the AST for the file that contains the calling frame first,
    # then the line number specified for the call is equal to or greater than
    # the line where the function call begins (this handles function calls that
    # span multiple lines), so find the last AST node whose lineno is not
    # greater than the lineno in the calling frame.
    call_line = inspect.getframeinfo(calling_frame[0]).lineno
    file_src = inspect.findsource(calling_frame[0])[0]
    ast_nodes = ast.parse(''.join(file_src))

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


def type_assert(val, *safe_types):
    """
    Raise an exception if val is not one of the declared safe types.
    Calling Example:
        type_assert(my_func, function)  # my_func must be a function
        type_assert(my_val, int, float)  # my_val must be int or float

    :raises: TypeError if type_check(val, safe_types) returns False
    :raises: ValueError if you call type_assert with an explicit value and not
        a variable as the first argument.
    """
    assert safe_types
    if not type_check(val, *safe_types):
        caller = inspect.stack()[1]
        name = _name_of_arg_at_caller(0)
        type_names = tuple(
            [t.__name__ if hasattr(t, "__name__") else t for t in safe_types]
        )
        raise TypeError(
            "{}:{} requires {} to be one of {}"
            .format(caller.filename, caller.function, name, type_names)
        )
