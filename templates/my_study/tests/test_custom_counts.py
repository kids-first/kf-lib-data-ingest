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


def test_count_family_groups():
    """
    Test that we have 1 duo and 1 trio families
    """
    selector = CONCEPT.FAMILY.ID + "::" + CONCEPT.PARTICIPANT.ID
    selected_data = extract_stage_data["links"].get(selector)
    assert selected_data

    expected_duos = 1
    expected_trios = 1
    actual_duos = 0
    actual_trios = 0

    for family_id, participant_ids in selected_data.items():
        if len(participant_ids) == 2:
            actual_duos += 1
        elif len(participant_ids) == 3:
            actual_trios += 1

    assert expected_duos == actual_duos
    assert expected_trios == expected_trios
