from conftest import concept_discovery_dict


extract_stage_data = concept_discovery_dict('ExtractStage')


def test_count_family_groups():
    """
    Test that we have 1 duo and 1 trio families
    """
    expected_duos = 1
    expected_trios = 1

    selector = 'CONCEPT|FAMILY|ID::CONCEPT|PARTICIPANT|ID'
    selected_data = extract_stage_data['links'].get(selector)
    assert selected_data

    actual_duos = 0
    actual_trios = 0
    for family_id, participant_ids in selected_data.items():
        if len(participant_ids) == 2:
            actual_duos += 1
        elif len(participant_ids) == 3:
            actual_trios += 1

    assert expected_duos == actual_duos
    assert expected_trios == expected_trios
