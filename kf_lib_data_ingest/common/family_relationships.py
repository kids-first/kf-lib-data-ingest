from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from pandas import concat, DataFrame, notna

P = CONCEPT.PARTICIPANT
FR = CONCEPT.FAMILY_RELATIONSHIP
FAM = CONCEPT.FAMILY


class FamilyException(Exception):
    pass


def _p1p2_relationships_from_motherfather_ids(relations_df):
    """Create P1->RELATION->P2 relationships from P.ID and MOTHER_ID/FATHER_ID.

    :param relations_df: a DataFrame containing PARTICIPANT.ID,
        PARTICIPANT.MOTHER_ID or PARTICIPANT.FATHER_ID, and optionally
        PARTICIPANT.GENDER and FAMILY_RELATIONSHIP.VISIBLE columns
    :type relations_df: DataFrame
    :rtype: DataFrame
    """

    def _link_column(column, relation):
        ret = relations_df[
            relations_df.columns.intersection(
                {P.ID, P.GENDER, column, FR.VISIBLE}
            )
        ].rename(
            columns={
                column: FR.PERSON1.ID,
                P.ID: FR.PERSON2.ID,
                P.GENDER: FR.PERSON2.GENDER,
            }
        )
        ret[FR.RELATION_FROM_1_TO_2] = relation
        ret[FR.PERSON1.GENDER] = constants.GENDER_FROM_RELATION[relation]
        return ret

    parents = [
        _link_column(P.MOTHER_ID, constants.RELATIONSHIP.MOTHER),
        _link_column(P.FATHER_ID, constants.RELATIONSHIP.FATHER),
    ]
    return (
        concat(parents)
        .dropna(subset=[FR.RELATION_FROM_1_TO_2, FR.PERSON1.ID, FR.PERSON2.ID])
        .drop_duplicates()
        .reset_index(drop=True)
    )


def _p1p2_relations_from_proband_relations(relations_df):
    """Create P1->RELATION->P2 relationships from non-probands to probands
    in the same family.

    :param relations_df: a DataFrame containing FAMILY.ID, PARTICIPANT.ID,
        PARTICIPANT.RELATIONSHIP_TO_PROBAND, and optionally PARTICIPANT.GENDER
        and FAMILY_RELATIONSHIP.VISIBLE columns
    :type relations_df: DataFrame
    :rtype: DataFrame
    """
    good_cols = {FAM.ID, P.ID, P.GENDER, P.RELATIONSHIP_TO_PROBAND, FR.VISIBLE}
    colsA = relations_df.columns.intersection(good_cols)
    colsB = colsA.difference({P.RELATIONSHIP_TO_PROBAND, FR.VISIBLE})

    non_probands = relations_df[
        relations_df[P.RELATIONSHIP_TO_PROBAND]
        != constants.RELATIONSHIP.PROBAND
    ][colsA].rename(
        columns={
            P.GENDER: FR.PERSON1.GENDER,
            P.ID: FR.PERSON1.ID,
            P.RELATIONSHIP_TO_PROBAND: FR.RELATION_FROM_1_TO_2,
        }
    )

    probands = relations_df[
        relations_df[P.RELATIONSHIP_TO_PROBAND]
        == constants.RELATIONSHIP.PROBAND
    ][colsB].rename(columns={P.GENDER: FR.PERSON2.GENDER, P.ID: FR.PERSON2.ID})

    r = non_probands.merge(probands, how="inner", on=FAM.ID)
    del r[FAM.ID]
    return r.drop_duplicates().reset_index(drop=True)


def _sibling_parent_relations_from_p1p2_relations(relations_df):
    """Create P1->RELATION->P2 relationships from siblings to parents.

    :param relations_df: a DataFrame containing FR.PERSON1.ID, FR.PERSON2.ID,
        FR.RELATION_FROM_1_TO_2, and optionally FR.PERSON1.GENDER,
        FR.PERSON2.GENDER, and FR.VISIBLE columns
    :type relations_df: DataFrame
    :rtype: DataFrame
    """
    df = relations_df.copy()

    def _link(sibling_roles, mother_role, father_role, generic_role):
        """Connect parents to designated siblings in the same family."""
        siblings = df[
            relations_df[FR.RELATION_FROM_1_TO_2].isin(sibling_roles)
        ].copy()

        if siblings.empty:
            return DataFrame()
        else:
            mothers = df[
                relations_df[FR.RELATION_FROM_1_TO_2] == mother_role
            ].copy()
            mothers[FR.RELATION_FROM_1_TO_2] = constants.RELATIONSHIP.MOTHER

            fathers = df[
                relations_df[FR.RELATION_FROM_1_TO_2] == father_role
            ].copy()
            fathers[FR.RELATION_FROM_1_TO_2] = constants.RELATIONSHIP.FATHER

            generics = df[
                relations_df[FR.RELATION_FROM_1_TO_2] == generic_role
            ].copy()
            generics[FR.RELATION_FROM_1_TO_2] = constants.RELATIONSHIP.PARENT

            parents = concat([mothers, fathers, generics])
            r = parents.merge(siblings, how="inner", on=FR.PERSON2.ID)
            r = r[
                r.columns.intersection(
                    {
                        FR.PERSON1.ID + "_x",
                        FR.PERSON1.GENDER + "_x",
                        FR.RELATION_FROM_1_TO_2 + "_x",
                        FR.PERSON1.ID + "_y",
                        FR.PERSON1.GENDER + "_y",
                    }
                )
            ].rename(
                columns={
                    FR.PERSON1.ID + "_x": FR.PERSON1.ID,
                    FR.PERSON1.GENDER + "_x": FR.PERSON1.GENDER,
                    FR.RELATION_FROM_1_TO_2 + "_x": FR.RELATION_FROM_1_TO_2,
                    FR.PERSON1.ID + "_y": FR.PERSON2.ID,
                    FR.PERSON1.GENDER + "_y": FR.PERSON2.GENDER,
                }
            )

            r = r.drop_duplicates().reset_index(drop=True)
            if FR.VISIBLE in df:
                r[FR.VISIBLE] = r.pop(FR.VISIBLE + "_x") & r.pop(
                    FR.VISIBLE + "_y"
                )

            return r

    dfs = [
        _link(p["siblings"], p["mother"], p["father"], p["generic"])
        for p in constants.RELATIONSHIP_PARENTS
    ]
    return concat(dfs).drop_duplicates().reset_index(drop=True)


def _bidirect_relationships(relations_df):
    """Add missing reverse relationships (e.g. if P1->P2 then also P2->P1)

    :param relations_df: a DataFrame containing PERSON1.ID, PERSON2.ID,
        RELATION_FROM_1_TO_2, and optionally PERSON2.GENDER columns
    :type relations_df: DataFrame
    :rtype: DataFrame
    """
    reverse_df = relations_df.copy()

    reverse_df[[FR.PERSON2.ID, FR.PERSON1.ID]] = relations_df[
        [FR.PERSON1.ID, FR.PERSON2.ID]
    ]
    reverse_df[FR.RELATION_FROM_1_TO_2].replace(
        constants.REVERSE_RELATIONSHIPS, inplace=True
    )
    if FR.PERSON2.GENDER in reverse_df:
        reverse_df[FR.RELATION_FROM_1_TO_2] = reverse_df.apply(
            lambda r: constants.genderize_relationship(
                r[FR.RELATION_FROM_1_TO_2], r[FR.PERSON2.GENDER]
            ),
            axis=1,
        )

    r = concat([relations_df, reverse_df])
    r = r.drop(columns=[FR.PERSON1.GENDER, FR.PERSON2.GENDER], errors="ignore")
    return r.drop_duplicates().reset_index(drop=True)


def _infer_genders_from_p1p2_relations(relations_df):
    """Add missing genders inferrable from known relationships and test for
    inconsistency.
    """
    _infer_genders_from_relation_column(
        relations_df, FR.PERSON1.ID, FR.PERSON1.GENDER, FR.RELATION_FROM_1_TO_2
    )
    if FR.PERSON2.GENDER in relations_df:
        # check for left-side/right-side gender mismatch
        left = relations_df[[FR.PERSON1.ID, FR.PERSON1.GENDER]]
        right = relations_df[[FR.PERSON2.ID, FR.PERSON2.GENDER]]
        df = left.merge(right, left_on=FR.PERSON1.ID, right_on=FR.PERSON2.ID)
        df = df[df[FR.PERSON1.GENDER] != df[FR.PERSON2.GENDER]].dropna()
        if not df.empty:
            errs = df.apply(
                lambda r: (
                    f"Participant {r[FR.PERSON1.ID]} can't be both"
                    f" {r[FR.PERSON1.GENDER]} and {r[FR.PERSON2.GENDER]}."
                ),
                axis=1,
            )
            errs = "\n".join(errs.values)
            raise FamilyException(
                f"Found errors in\n\n{relations_df.to_string()}\n\n{errs}"
            )


def _infer_genders_from_relation_column(
    relations_df, id_col, gender_col, relation_col
):
    """Add missing genders inferrable from known relationships and test for
    inconsistency.
    """
    df = relations_df.copy()
    df[gender_col] = (
        relations_df[relation_col]
        .map(constants.GENDER_FROM_RELATION)
        .where(notna, other=relations_df.get(gender_col))
    )

    if gender_col in relations_df:
        invalid = relations_df.loc[
            relations_df[gender_col][relations_df[gender_col] != df[gender_col]]
            .dropna()
            .index
        ]
        if not invalid.empty:
            errs = invalid.apply(
                lambda r: f"In row {r.name}: Participant '{r[id_col]}' can't be"
                f" both {r[gender_col]} and also a"
                f" {r[relation_col]}.",
                axis=1,
            )
            errs = "\n".join(errs)
            raise FamilyException(
                f"Found errors in\n\n{relations_df.to_string()}\n\n{errs}"
            )

    relations_df[gender_col] = df[gender_col]


def _infer_genders_from_motherfather_relations(relations_df):
    """Add missing genders inferrable from known relationships and test for
    inconsistency.
    """
    mothers = relations_df[P.MOTHER_ID].dropna()
    fathers = relations_df[P.FATHER_ID].dropna()
    inferences = dict(
        {i: constants.GENDER.FEMALE for i in mothers},
        **{i: constants.GENDER.MALE for i in fathers},
    )
    df = (
        relations_df[P.ID]
        .map(inferences)
        .where(notna, other=relations_df.get(P.GENDER))
    )

    if P.GENDER in relations_df:
        invalid = relations_df.loc[
            relations_df[P.GENDER][relations_df[P.GENDER] != df.values]
            .dropna()
            .index
        ]
        if not invalid.empty:

            def _err_msg(r):
                m = (
                    f"Participant '{r[P.ID]}' (row {r.name}) can't have gender"
                    f" '{r[P.GENDER]}'"
                )
                if (r[P.GENDER] != constants.GENDER.MALE) and (
                    r[P.ID] in fathers.values
                ):
                    m += " and also be father of "
                    m += ", ".join(
                        [
                            f"'{relations_df[P.ID].loc[i]}' (row {i})"
                            for i in fathers[fathers == r[P.ID]].index
                        ]
                    )
                if (r[P.GENDER] != constants.GENDER.FEMALE) and (
                    r[P.ID] in mothers.values
                ):
                    m += " and also be mother of "
                    m += ", ".join(
                        [
                            f"'{relations_df[P.ID].loc[i]}' (row {i})"
                            for i in fathers[fathers == r[P.ID]].index
                        ]
                    )
                return m

            errs = "\n".join(invalid.apply(_err_msg, axis=1))
            raise FamilyException(
                f"Found errors in\n\n{relations_df.to_string()}\n\n{errs}"
            )

    relations_df[P.GENDER] = df


def convert_relationships_to_p1p2(
    relations_df, infer_genders=False, bidirect=False
):
    """Convert non-P1->RELATION->P2 relationships into P1->RELATION->P2
    relationships and make all relationships bidirectional.

    :param relations_df: a DataFrame containing a meaningful combination of
        FAMILY.ID, PARTICIPANT.ID, PARTICIPANT.RELATIONSHIP_TO_PROBAND,
        PARTICIPANT.MOTHER_ID, PARTICIPANT.FATHER_ID, PARTICIPANT.GENDER,
        FAMILY_RELATIONSHIP.PERSON1.ID, FAMILY_RELATIONSHIP.PERSON1.GENDER,
        FAMILY_RELATIONSHIP.PERSON2.ID, FAMILY_RELATIONSHIP.PERSON2.GENDER,
        FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2, and
        FAMILY_RELATIONSHIP.VISIBLE columns
    :type relations_df: DataFrame
    :param infer_genders: Should missing participant gender information be
        inferred from the given relationships in order to produce gendered
        relationships when converting and expanding the relationship set.
    :type infer_genders: bool
    :param bidirect: Should symmetric relationships be constructed in both
        directions.
    :type bidirect: bool
    :rtype: DataFrame
    """
    good_ones = []

    # Convert FAMILY,PID,REL_TO_PROBAND into P1,REL_1_TO_2,P2.
    if (
        (FAM.ID in relations_df)
        and (P.ID in relations_df)
        and (P.RELATIONSHIP_TO_PROBAND in relations_df)
    ):
        if infer_genders:
            _infer_genders_from_relation_column(
                relations_df, P.ID, P.GENDER, P.RELATIONSHIP_TO_PROBAND
            )
        good_ones.append(_p1p2_relations_from_proband_relations(relations_df))

    # Convert PID,MOTHER/FATHER_ID into P1,REL_1_TO_2,P2.
    if (P.ID in relations_df) and (
        (P.MOTHER_ID in relations_df) or (P.FATHER_ID in relations_df)
    ):
        in_both = set(relations_df[P.MOTHER_ID].dropna()).intersection(
            set(relations_df[P.FATHER_ID].dropna())
        )
        if in_both:
            msg = "PARTICIPANT.IDs may not be both a MOTHER ID and a FATHER ID:"
            msg += f"\n\n{in_both}"
            raise FamilyException(
                f"Found errors in\n\n{relations_df.to_string()}\n\n{msg}"
            )

        if infer_genders:
            _infer_genders_from_motherfather_relations(relations_df)

        good_ones.append(
            _p1p2_relationships_from_motherfather_ids(relations_df)
        )

    # These ones were already in P1,REL_1_TO_2,P2 format.
    if (
        (FR.PERSON1.ID in relations_df)
        and (FR.RELATION_FROM_1_TO_2 in relations_df)
        and (FR.PERSON2.ID in relations_df)
    ):
        good_cols = {
            FR.PERSON1.ID,
            FR.PERSON1.GENDER,
            FR.RELATION_FROM_1_TO_2,
            FR.PERSON2.ID,
            FR.PERSON2.GENDER,
            FR.VISIBLE,
        }
        good_cols = relations_df.columns.intersection(good_cols)
        already_good = (
            relations_df[good_cols].drop_duplicates().dropna(how="all")
        )
        if infer_genders:
            _infer_genders_from_p1p2_relations(already_good)

        good_ones.append(already_good)

    good_ones = concat(good_ones).drop_duplicates()

    if (
        (FR.PERSON1.ID in good_ones)
        and (FR.RELATION_FROM_1_TO_2 in good_ones)
        and (FR.PERSON2.ID in good_ones)
    ):
        # Fill out child parent relations
        filled = _sibling_parent_relations_from_p1p2_relations(good_ones)
        good_ones = concat([good_ones, filled]).drop_duplicates()

        if bidirect:
            # Make sure all relationships are bidirectional.
            good_ones = _bidirect_relationships(good_ones)

    return good_ones.dropna(how="all")
