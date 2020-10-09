# This is just an enumeration of all the cardinality terms
class RELATIONS:
    (
        ONE,
        MANY,
        ONEZERO,
        ANY,
        ONE_ONE,
        ONE_MANY,
        ONE_ONEZERO,
        ONE_ANY,
        MANY_ONE,
        MANY_MANY,
        MANY_ONEZERO,
        MANY_ANY,
        ONEZERO_ONE,
        ONEZERO_MANY,
        ONEZERO_ONEZERO,
        ONEZERO_ANY,
        ANY_ONE,
        ANY_MANY,
        ANY_ONEZERO,
        ANY_ANY,
    ) = range(1, 21)


R = RELATIONS


# Each cardinality test has two directions. Each direction gets its own test.
COMPARATORS = {
    R.ONE: ("exactly 1", lambda x: x == 1),
    R.MANY: ("at least 1", lambda x: x >= 1),
    R.ONEZERO: ("at most 1", lambda x: x <= 1),
    # There are no tests for ANY, because everything would just pass
}

# The right-side cardinality tests
TESTS = {
    R.ONE_ONE: COMPARATORS[R.ONE],
    R.ONE_MANY: COMPARATORS[R.MANY],
    R.ONE_ONEZERO: COMPARATORS[R.ONEZERO],
    R.MANY_ONE: COMPARATORS[R.ONE],
    R.MANY_MANY: COMPARATORS[R.MANY],
    R.MANY_ONEZERO: COMPARATORS[R.ONEZERO],
    R.ONEZERO_ONE: COMPARATORS[R.ONE],
    R.ONEZERO_MANY: COMPARATORS[R.MANY],
}

# The left-side cardinality tests
REVERSE_TESTS = {
    R.ONE_ONE: COMPARATORS[R.ONE],
    R.ONE_MANY: COMPARATORS[R.ONE],
    R.ONE_ONEZERO: COMPARATORS[R.ONE],
    R.MANY_ONE: COMPARATORS[R.MANY],
    R.MANY_MANY: COMPARATORS[R.MANY],
    R.MANY_ONEZERO: COMPARATORS[R.MANY],
    R.ONEZERO_ONE: COMPARATORS[R.ONEZERO],
    R.ONEZERO_MANY: COMPARATORS[R.ONEZERO],
    R.MANY_ANY: COMPARATORS[R.MANY],
}
