"""
Auto-generated test module

Replace the contents of this with your data validation tests

See documentation at
https://kids-first.github.io/kf-lib-data-ingest/ for information on
implementing data validation tests.
"""
from conftest import concept_discovery_dict
from kf_lib_data_ingest.common.concept_schema import CONCEPT

extract_stage_data = concept_discovery_dict("ExtractStage")


def test_family_count():
    """
    Test that we have at least 2 families
    """
    selector = CONCEPT.FAMILY.ID
    selected_data = extract_stage_data['sources'].get(selector)
    assert selected_data

    expected_families = 3
    actual_families = len(selected_data.keys())

    assert actual_families == expected_families
