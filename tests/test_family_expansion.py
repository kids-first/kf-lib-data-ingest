import pandas
import pytest
from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.family_relationships import (
    convert_relationships_to_p1p2,
    FamilyException,
)
from pandas import DataFrame, concat
from pandas.testing import assert_frame_equal

P = CONCEPT.PARTICIPANT
FR = CONCEPT.FAMILY_RELATIONSHIP
FAM = CONCEPT.FAMILY

G = constants.GENDER
R = constants.RELATIONSHIP

P1P2_RELATIONS = DataFrame(
    {
        FR.PERSON1.ID: ["AA", "BB", "CC", "DD"],
        FR.PERSON1.GENDER: [G.MALE, G.FEMALE, G.FEMALE, G.OTHER],
        FR.RELATION_FROM_1_TO_2: [
            R.FATHER,
            R.PATERNAL_GRANDMOTHER,
            R.PATERNAL_GRANDMOTHER,
            R.PARENT,
        ],
        FR.PERSON2.ID: ["EE", "EE", "FF", "GG"],
        FR.PERSON2.GENDER: [G.OTHER, G.OTHER, G.MALE, G.FEMALE],
    }
)

P1P2_RESULT = DataFrame(
    {
        FR.PERSON1.ID: [
            "AA",
            "BB",
            "CC",
            "DD",
            "EE",
            "EE",
            "FF",
            "GG",
            "AA",
            "BB",
        ],
        FR.RELATION_FROM_1_TO_2: [
            R.FATHER,
            R.PATERNAL_GRANDMOTHER,
            R.PATERNAL_GRANDMOTHER,
            R.PARENT,
            R.CHILD,
            R.PATERNAL_GRANDCHILD,
            R.PATERNAL_GRANDSON,
            R.DAUGHTER,
            R.SON,
            R.MOTHER,
        ],
        FR.PERSON2.ID: [
            "EE",
            "EE",
            "FF",
            "GG",
            "AA",
            "BB",
            "CC",
            "DD",
            "BB",
            "AA",
        ],
    }
)

MOTHERFATHER_RELATIONS = DataFrame(
    {
        P.ID: ["A", "B", "C", "D", "E", "F", "G"],
        P.GENDER: [
            G.MALE,
            G.FEMALE,
            G.FEMALE,
            G.MALE,
            G.MALE,
            G.FEMALE,
            None,
        ],
        P.MOTHER_ID: ["B", "C", None, None, None, None, None],
        P.FATHER_ID: ["D", "E", None, None, None, "G", None],
    }
)

MOTHERFATHER_RESULT = DataFrame(
    {
        FR.PERSON1.ID: ["A", "A", "B", "B", "B", "C", "D", "E", "F", "G"],
        FR.RELATION_FROM_1_TO_2: [
            R.SON,
            R.SON,
            R.MOTHER,
            R.DAUGHTER,
            R.DAUGHTER,
            R.MOTHER,
            R.FATHER,
            R.FATHER,
            R.DAUGHTER,
            R.FATHER,
        ],
        FR.PERSON2.ID: ["B", "D", "A", "C", "E", "B", "A", "B", "G", "F"],
    }
)

NULLIFIED_MOTHERFATHER_RESULT = MOTHERFATHER_RESULT.copy()
NULLIFIED_MOTHERFATHER_RESULT[FR.RELATION_FROM_1_TO_2] = [
    R.CHILD,
    R.CHILD,
    R.MOTHER,
    R.DAUGHTER,
    R.DAUGHTER,
    R.MOTHER,
    R.FATHER,
    R.FATHER,
    R.CHILD,
    R.FATHER,
]

PROBAND_RELATIONS = DataFrame(
    {
        P.ID: ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG"],
        FAM.ID: ["1", "1", "1", "1", "2", "2", "3"],
        P.RELATIONSHIP_TO_PROBAND: [
            R.PROBAND,
            R.SISTER,
            R.MOTHER,
            R.MATERNAL_GRANDMOTHER,
            R.DAUGHTER,
            R.PROBAND,
            R.PROBAND,
        ],
    }
)

PROBAND_RESULT = DataFrame(
    {
        FR.PERSON1.ID: [
            "AAA",
            "AAA",
            "AAA",
            "BBB",
            "BBB",
            "CCC",
            "CCC",
            "CCC",
            "DDD",
            "DDD",
            "EEE",
            "FFF",
        ],
        FR.RELATION_FROM_1_TO_2: [
            R.SIBLING,
            R.CHILD,
            R.MATERNAL_GRANDCHILD,
            R.SISTER,
            R.DAUGHTER,
            R.MOTHER,
            R.MOTHER,
            R.DAUGHTER,
            R.MATERNAL_GRANDMOTHER,
            R.MOTHER,
            R.DAUGHTER,
            R.PARENT,
        ],
        FR.PERSON2.ID: [
            "BBB",
            "CCC",
            "DDD",
            "AAA",
            "CCC",
            "AAA",
            "BBB",
            "DDD",
            "AAA",
            "CCC",
            "FFF",
            "EEE",
        ],
    }
)


def compare(input, output, expected):
    try:
        output = output.sort_values(
            by=[FR.PERSON1.ID, FR.PERSON2.ID]
        ).reset_index(drop=True)
        expected = expected.sort_values(
            by=[FR.PERSON1.ID, FR.PERSON2.ID]
        ).reset_index(drop=True)
        assert_frame_equal(output, expected, check_like=True)
    except Exception:
        pandas.set_option("display.max_rows", None)
        pandas.set_option("display.max_columns", None)
        pandas.set_option("display.width", 2000)
        pandas.set_option("display.float_format", "{:20,.2f}".format)
        pandas.set_option("display.max_colwidth", None)
        print("input")
        print(input)
        print("output")
        print(output)
        print("expected")
        print(expected)
        pandas.reset_option("display.max_rows")
        pandas.reset_option("display.max_columns")
        pandas.reset_option("display.width")
        pandas.reset_option("display.float_format")
        pandas.reset_option("display.max_colwidth")
        raise


def gender_nullification(input, gender_col, expected):
    input = input.copy()
    input[gender_col] = None
    output = convert_relationships_to_p1p2(
        input, infer_genders=True, bidirect=True
    )
    compare(input, output, expected)

    # invalid genders for the given relationships
    input[gender_col] = constants.GENDER.OTHER
    with pytest.raises(FamilyException):
        convert_relationships_to_p1p2(input, infer_genders=True, bidirect=True)

    del input[gender_col]
    output = convert_relationships_to_p1p2(
        input, infer_genders=True, bidirect=True
    )
    compare(input, output, expected)


def test_p1p2():
    input = P1P2_RELATIONS
    output = convert_relationships_to_p1p2(
        input, infer_genders=True, bidirect=True
    )
    expected = P1P2_RESULT
    compare(input, output, expected)

    gender_nullification(input, FR.PERSON1.GENDER, expected)

    # BB can't be both Female and Male
    input = input.copy()
    input[FR.PERSON2.ID][2] = "BB"
    with pytest.raises(FamilyException):
        convert_relationships_to_p1p2(input, infer_genders=True, bidirect=True)


def test_motherfather():
    input = MOTHERFATHER_RELATIONS
    output = convert_relationships_to_p1p2(
        input, infer_genders=True, bidirect=True
    )
    expected = MOTHERFATHER_RESULT
    compare(input, output, expected)

    expected = NULLIFIED_MOTHERFATHER_RESULT
    gender_nullification(input, P.GENDER, expected)


def test_proband():
    input = PROBAND_RELATIONS
    output = convert_relationships_to_p1p2(
        input, infer_genders=True, bidirect=True
    )
    expected = PROBAND_RESULT
    compare(input, output, expected)

    gender_nullification(input, P.GENDER, expected)


def test_p1p2motherfather():
    input = concat([P1P2_RELATIONS, MOTHERFATHER_RELATIONS])
    output = convert_relationships_to_p1p2(
        input, infer_genders=True, bidirect=True
    )
    expected = concat([P1P2_RESULT, MOTHERFATHER_RESULT])
    compare(input, output, expected)

    expected = concat([P1P2_RESULT, NULLIFIED_MOTHERFATHER_RESULT])
    gender_nullification(input, P.GENDER, expected)


def test_p1p2proband():
    input = concat([P1P2_RELATIONS, PROBAND_RELATIONS])
    output = convert_relationships_to_p1p2(
        input, infer_genders=True, bidirect=True
    )
    expected = concat([P1P2_RESULT, PROBAND_RESULT])
    compare(input, output, expected)

    gender_nullification(input, P.GENDER, expected)


def test_p1p2motherfatherproband():
    input = concat([P1P2_RELATIONS, MOTHERFATHER_RELATIONS, PROBAND_RELATIONS])
    output = convert_relationships_to_p1p2(
        input, infer_genders=True, bidirect=True
    )
    expected = concat([P1P2_RESULT, MOTHERFATHER_RESULT, PROBAND_RESULT])
    compare(input, output, expected)

    expected = concat(
        [P1P2_RESULT, NULLIFIED_MOTHERFATHER_RESULT, PROBAND_RESULT]
    )
    gender_nullification(input, P.GENDER, expected)
