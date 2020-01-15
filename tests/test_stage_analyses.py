from random import choice, randint
from string import ascii_lowercase

from kf_lib_data_ingest.common.concept_schema import (
    concept_from,
    concept_set,
    str_to_CONCEPT,
)
from kf_lib_data_ingest.etl.stage_analyses import (
    NO,
    NO_EXPECTED_COUNTS,
    YES,
    check_counts,
)


def randomString():
    n = randint(5, 10)
    return "".join(choice(ascii_lowercase) for i in range(n))


def randomThing():
    key = randomString()
    values = set(randomString() for i in range(randint(5, 10)))
    return key, values


def test_invalid_str_key():
    fake_sources = {
        c.ID: {k: v for k, v in [randomThing() for i in range(randint(5, 10))]}
        for c in concept_set
    }

    # no expected counts
    # should pass
    expected_counts = {}
    passed, messages = check_counts(fake_sources, expected_counts)
    assert passed
    assert all(YES not in m for m in messages)
    assert all(NO not in m for m in messages)
    assert NO_EXPECTED_COUNTS in messages

    # all matching
    # should pass
    expected_counts = {k: len(v) for k, v in fake_sources.items()}
    passed, messages = check_counts(fake_sources, expected_counts)
    assert expected_counts.keys() == fake_sources.keys()
    assert passed
    assert NO_EXPECTED_COUNTS not in messages
    assert any(YES in m for m in messages)
    assert all(NO not in m for m in messages)

    # all matching but with concept names instead of attributes
    # should pass
    expected_counts = {concept_from(k): v for k, v in expected_counts.items()}
    passed, messages = check_counts(fake_sources, expected_counts)
    assert passed
    assert expected_counts.keys() != fake_sources.keys()

    # all matching but with concept classes
    # should pass
    expected_counts = {str_to_CONCEPT[k]: v for k, v in expected_counts.items()}
    passed, messages = check_counts(fake_sources, expected_counts)
    assert passed
    assert expected_counts.keys() != fake_sources.keys()

    # matching keys, not matching values
    # should fail
    expected_counts = {k: v + 1 for k, v in expected_counts.items()}
    passed, messages = check_counts(fake_sources, expected_counts)
    assert not passed

    # fake keys
    # should fail (but not error)
    expected_counts = {"FAKE": 5}
    passed, messages = check_counts(fake_sources, expected_counts)
    assert not passed
