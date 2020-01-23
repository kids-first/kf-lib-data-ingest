"""
A class which first validates and then stores transformation configuration
parameters from a target API configuration module.

An instance of this class is used by the transform stage to populate instances
of target model concepts (i.e. participants, diagnoses, etc) with data before
those instances are loaded into the target service.

This module must have the following required attributes:

    - target_service_entity_id
    - target_concepts

All but the first of these required attributes must be dicts.

    - target_service_entity_id
      A string containing the name of the unique identifier atttribute a
      target entity. This would likely be a primary key in the target service
      and the attribute used in CRUD operations for this entity.

    - target_concepts
      A dict of dicts. Each inner dict is a target concept dict, which
      contains mappings of target concept properties to standard concept
      attributes.

        A target concept dict has the following schema:

            Required Keys:
                - standard_concept
                - properties
                - endpoint

            Optional Keys:
                - links

        {
            'standard_concept':
                type: etl.common.concept_schema.CONCEPT
                example: CONCEPT.PARTICIPANT
                description: the standard concept that this target concept
                maps to.

            'properties':
                type: dict

                    key: a string containing the target concept property
                    value: a standard concept attribute from
                    etl.common.concept_schema OR a tuple
                    of the form
                    (standard concept attribute, a type from ALLOWABLE_TYPES)

                example:
                {
                    'external_id': CONCEPT.PARTICIPANT.ID,
                    'race': CONCEPT.PARTICIPANT.RACE,
                    'is_proband': (CONCEPT.PARTICIPANT.IS_PROBAND, bool)
                }
                description: the target concept property mappings to standard
                concept attributes
            },
            links:
                type: list of dicts

                    dict content:
                        - target_attribute: a string containing the target
                        concept property
                        - standard concept: a standard concept from
                        etl.common.concept_schema
                        - target_concept: name of the linked target concept

                    dict example:
                    {
                        'target_attribute': 'family_id',
                        'standard_concept': CONCEPT.FAMILY,
                        'target_concept': 'family'
                    }
                description: identifiers which map to standard concept
                UNIQUE_KEY attributes. These represent foreign keys in the
                target model.
            ,
            'endpoint':
                type: string
                example: '/participants'
                description: the CRUD endpoint for the concept in the target
                service
        }

        It may seem unecessary to separate the mappings into
        'properties', and 'links', but this is important because the
        mappings in 'links' are treated differently than those in
        'properties'.

        The value in a key, value pair in a links dict, during the load
        stage, will be translated into a target model ID. A value in a
        key, value pair in the 'properties' dict will be kept as is during the
        load stage.

        For example, before ID translation in load stage, the 'links' list
        could be:

            'links': [{
                'target_concept': 'participant',
                'participant_id': 'P1'
            }]
        And after ID translation occurs, the 'links' list will be translated
        into:

            'links': {
                'participant_id': 'PT_00001111'
            }

"""
from pprint import pformat


from kf_lib_data_ingest.etl.configuration.base_config import PyModuleConfig
from kf_lib_data_ingest.common.concept_schema import (
    concept_property_set,
    concept_set,
)
from kf_lib_data_ingest.common.type_safety import (
    assert_safe_type,
    assert_all_safe_type,
)


class TargetAPIConfig(PyModuleConfig):
    def __init__(self, filepath):
        """
        Import python module, validate it, and populate instance attributes.
        """
        super().__init__(filepath)
        self.target_concepts = self.contents.target_concepts

    def _validate(self):
        """
        Validate that the target API config Python module has all the required
        content with the right types.
        """
        self._validate_required_attrs()
        self._validate_target_concepts()

        if hasattr(self.contents, "validate"):
            getattr(self.contents, "validate")()

    def _validate_required_attrs(self):
        """
        Check that all module level required attributes are present and are
        of the correct form.
        """
        required_dict_attrs = ["target_concepts"]
        required = required_dict_attrs + ["target_service_entity_id"]
        for attr in required:
            if not hasattr(self.contents, attr):
                raise AttributeError(
                    f"Missing one of the required keys: {attr}"
                )

        # Check that required attributes that are supposed to be dicts
        # are actually dicts
        required_dicts = [
            getattr(self.contents, attr) for attr in required_dict_attrs
        ]
        assert_all_safe_type(required_dicts, dict)

        # Check that target_service_entity_id is a string
        assert_safe_type(self.contents.target_service_entity_id, str)

    def _validate_target_concepts(self):
        """
        Validate target concept dicts in target_concepts.
        Check the format and content of each dict.
        """
        # Check that all required keys are present
        self._validate_required_keys()

        # Check that all mapped concepts are all valid standard concepts
        self._validate_mapped_standard_concepts()

        # Check that all properties are mapped to valid standard concept attrs
        self._validate_target_concept_attr_mappings()

        # Check that all endpoints are strings
        self._validate_endpoints()

    def _validate_required_keys(self):
        """
        Check that every target concept dict has the required keys:
            - standard_concept
            - properties
            - endpoint
        """
        target_concepts = self.contents.target_concepts

        required_keys = {"standard_concept", "properties", "endpoint"}
        for target_concept, target_concept_dict in target_concepts.items():
            for required_key in required_keys:
                if required_key not in target_concept_dict.keys():
                    raise KeyError(
                        f"Dict for {target_concept} is missing one of "
                        f"target concept dict required keys: {required_key}"
                    )

    def _validate_mapped_standard_concepts(self):
        """
        Check that each target dict has a valid mapping for
        standard_concept.

        The mapped value must be an existing standard concept
        in etl.common.concept_schema.
        """
        mapped_concepts = [
            target_concept_dict.get("standard_concept")
            for target_concept_dict in self.contents.target_concepts.values()
        ]
        for mapped_concept in mapped_concepts:
            if mapped_concept not in concept_set:
                raise ValueError(
                    f"The mapped standard concept: {mapped_concept} does not "
                    "exist in the standard concept set!"
                )

    def _validate_target_concept_attr_mappings(self):
        """
        Validate target concept attribute mappings

        All target concept attributes must be strings.
        All mapped values must be valid standard concept attributes in
        etl.common.concept_schema.
        """
        target_concepts = self.contents.target_concepts

        # For each target_concept_dict
        for target_concept, target_concept_dict in target_concepts.items():
            # Validate properties
            self._validate_properties(target_concept, target_concept_dict)
            # Validate links
            if "links" in target_concept_dict:
                self._validate_link_list(target_concept_dict)

    def _validate_properties(self, target_concept, target_concept_dict):
        """
        Validate properties dict for each target concept config dict.

        Keys must be strs.
        Values must be existing standard concept strings.
        """
        props_dict = target_concept_dict["properties"]

        # All property keys are strs
        assert_all_safe_type(props_dict.keys(), str)

        # Mapped value must either be a tuple of the form
        # (standard concept attribute str, a function/callable) OR
        # standard concept attribute str
        for target_attr, value in props_dict.items():
            if value is None:
                continue

            # Check type mapping function/callable
            assert_safe_type(value, str, tuple)
            if isinstance(value, tuple):
                value, type_map_func = value
                assert_safe_type(type_map_func, callable)

            if value not in concept_property_set:
                raise ValueError(
                    f"Error in dict for {target_concept}. "
                    "All target concept attributes must be mapped to "
                    "an existing standard concept attribute. Mapped "
                    f"attribute {value} for target attr "
                    f"{target_concept}.{target_attr} does not exist."
                )

    def _validate_link_list(self, target_concept_dict):
        """
        Validate the 'links' element in a target concept's config dict

        'links' must be a list of dicts
        Each dict must have the required keys:
            - target_attribute
            - standard_concept
            - target_concept
        'target_attribute' must be a string
        'standard_concept' must point to an existing standard concept
        'target_concept' must point to an existing target_concept
        """
        valid_target_concepts = set(self.contents.target_concepts.keys())
        links = target_concept_dict["links"]
        assert_safe_type(links, list)
        assert_all_safe_type(links, dict)

        for link_dict in links:
            # Check link dict keys
            required_link_keys = {
                "target_attribute",
                "standard_concept",
                "target_concept",
            }
            if not all([k in link_dict for k in required_link_keys]):
                raise KeyError(
                    f"Badly formatted link dict:\n"
                    f"{target_concept_dict}\nMissing required keys:\n"
                    f"{required_link_keys}"
                )
            # Check target_attribute
            assert_safe_type(link_dict["target_attribute"], str)
            # Check standard concept
            sc = link_dict["standard_concept"]
            if sc not in concept_set:
                sc_name = sc
                try:
                    sc_name = sc._CONCEPT_NAME
                except AttributeError:
                    pass
                raise ValueError(
                    "Invalid link found in target concept config \n"
                    f"{pformat(target_concept_dict)}"
                    f"\nThe linked standard concept: {sc_name} does "
                    "not exist in the standard concept set!"
                )
            # Linked target concepts must be one of the target concept keys
            tc = link_dict["target_concept"]
            if tc not in valid_target_concepts:
                raise ValueError(
                    "Invalid link found in target concept config "
                    f"{pformat(target_concept_dict)}\nThe value of a "
                    "linked target concept must point to one of the "
                    "existing target concepts: "
                    f"{pformat(valid_target_concepts)}\n Target "
                    f"concept {tc} does not exist."
                )

    def _validate_endpoints(self):
        """
        Check that all endpoints are strings
        """
        endpoints = [
            target_concept_dict.get("endpoint")
            for target_concept_dict in self.contents.target_concepts.values()
        ]

        assert_all_safe_type(endpoints, str)
