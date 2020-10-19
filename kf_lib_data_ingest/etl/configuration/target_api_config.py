"""
Configuration module specifying how a target model maps to the standard model.

Your target API configuration module must contain a list named `all_targets`
which contains, in the order that you want them to be loaded into the target
service, target entity builder classes (not instances) of the form:

    class Foo:
        class_name = 'foo'
        target_id_concept = CONCEPT.FOO.TARGET_SERVICE_ID

        @classmethod
        def transform_records_list(cls, records_list):
            '''
            Transforms the given record list into the form needed for this
            class's build_key and build_entity methods.

            Defining this method is optional for your entity builder classes.

            :param records_list: list of records coming from the Transform stage
            :type records_list: list of dicts
            :return: list of reformatted records needed by this class's build_key
                and build_entity methods
            :rtype: list of dicts
            '''
            return new_records_list

        @classmethod
        def get_key_components(cls, record, get_target_id_from_record):
            '''
            Composes a minimal payload subset that uniquely identifies the given record.

            :param record: CONCEPT values representing one record of extracted data
            :type record: dict
            :param get_target_id_from_record: a function that, given input arguments
                (entity_class, record), will return the unique reference identifier
                assigned by the target service for that entity
            :type get_target_id_from_record: function
            :return: body of record components that uniquely identify the entity
                in the dataset
            :rtype: object
            :raises: Exception if record is not valid for entity
            '''
            return unique_key_components_from_record

        @classmethod
        def query_target_ids(cls, host, key_components):
            '''
            Ask the server for identifiers matching the given unique key components.

            :param host: host url
            :type host: str
            :param key_components: return value from get_key_components
            :type key_components: object
            :return: all identifiers on the server that match the key components
            :rtype: list
            '''
            return list_of_target_ids

        @classmethod
        def build_entity(cls, record, get_target_id_from_record):
            '''
            Constructs a payload body that can be submitted to the target service
            for the given record.

            :param record: CONCEPT values representing one record of extracted data
            :type record: dict
            :param get_target_id_from_record: a function that, given input arguments
                (entity_class, record), will return the unique reference identifier
                assigned by the target service for that entity
            :type get_target_id_from_record: function
            :return: an entity body ready to send to the target service
            '''
            return payload_body_composed_from_record

        @classmethod
        def submit(cls, host, body):
            '''
            Negotiate submitting completed entity data to the target service and
            return the identifier assigned by the server.

            :param host: host url
            :type host: str
            :param body: entity body constructed by entity_class.build_entity
            :return: The target entity reference ID that the service says was
                created or updated
            :rtype: str
            :raise: RequestException on error
            '''
            return unique_identifier_from_the_server_of_the_constructed_entity

The all_targets list will look like this:

    all_targets = [
        Foo,
        ...
    ]

Your entity classes can do anything else you want as long as they meet those
minimum requirements.
"""

from collections import defaultdict
from inspect import isclass, ismethod, isfunction, signature
from pprint import pformat

from kf_lib_data_ingest.etl.configuration.base_config import (
    ConfigValidationError,
    PyModuleConfig,
)


class TargetAPIConfig(PyModuleConfig):
    def _validate(self):
        if not isinstance(self.all_targets, list):
            raise ConfigValidationError(
                f"Target API Config module {self.config_filepath} is missing"
                " required `all_targets` list of target entity classes."
            )

        invalid_targets = defaultdict(list)
        for t in self.all_targets:
            if not isclass(t):
                invalid_targets["not a class"].append(t)
            else:
                if not hasattr(t, "class_name"):
                    invalid_targets["no class_name attribute"].append(t)

                if not hasattr(t, "target_id_concept"):
                    invalid_targets["no target_id_concept attribute"].append(t)

                if not hasattr(t, "build_entity"):
                    invalid_targets["no build_entity method"].append(t)
                elif not ismethod(t.build_entity):
                    invalid_targets["build_entity not a method"].append(t)
                elif not (
                    list(signature(t.build_entity).parameters)
                    == ["record", "get_target_id_from_record"]
                ):
                    invalid_targets[
                        "build_entity takes wrong input arguments"
                    ].append(t)

                if not hasattr(t, "get_key_components"):
                    invalid_targets["no get_key_components method"].append(t)
                elif not ismethod(t.get_key_components):
                    invalid_targets["get_key_components not a method"].append(t)
                elif not (
                    list(signature(t.get_key_components).parameters)
                    == ["record", "get_target_id_from_record"]
                ):
                    invalid_targets[
                        "get_key_components takes wrong input arguments"
                    ].append(t)

                if not hasattr(t, "submit"):
                    invalid_targets["no submit method"].append(t)
                elif not ismethod(t.submit):
                    invalid_targets["submit not a method"].append(t)
                elif not (
                    list(signature(t.submit).parameters) == ["host", "body"]
                ):
                    invalid_targets[
                        "submit takes wrong input arguments"
                    ].append(t)

                if hasattr(t, "query_target_ids"):
                    if not ismethod(t.query_target_ids):
                        invalid_targets["query_target_ids not a method"].append(
                            t
                        )
                    elif not (
                        list(signature(t.query_target_ids).parameters)
                        == ["host", "key_components"]
                    ):
                        invalid_targets[
                            "query_target_ids takes wrong input arguments"
                        ].append(t)

                if hasattr(t, "transform_records_list"):
                    if not ismethod(t.transform_records_list):
                        invalid_targets[
                            "transform_records_list not a method"
                        ].append(t)
                    elif not (
                        list(signature(t.transform_records_list).parameters)
                        == ["records_list"]
                    ):
                        invalid_targets[
                            "transform_records_list takes wrong input arguments"
                        ].append(t)

        if invalid_targets:
            raise ConfigValidationError(
                "The all_targets list must contain only classes of the form:\n"
                "\n"
                "class Foo:\n"
                "    class_name = 'foo'\n"
                "    target_id_concept = CONCEPT.FOO.TARGET_SERVICE_ID\n"
                "\n"
                "    @classmethod\n"
                "    def transform_records_list(cls, records_list):\n"
                "        # this method is optional\n"
                "        return new_records_list\n"
                "\n"
                "    @classmethod\n"
                "    def get_key_components(cls, record, get_target_id_from_record):\n"
                "        return unique_key_components_from_record\n"
                "\n"
                "    @classmethod\n"
                "    def query_target_ids(cls, host, key_components):\n"
                "        return list_of_target_ids\n"
                "\n"
                "    @classmethod\n"
                "    def build_entity(cls, record, get_target_id_from_record):\n"
                "        return payload_body_composed_from_record\n"
                "\n"
                "    @classmethod\n"
                "    def submit(cls, host, body):\n"
                "        return unique_identifier_from_the_server_of_the_constructed_entity\n"
                "\n\n"
                f"Errors found:\n {pformat(dict(invalid_targets))}"
            )

        if isfunction(self.validate):
            self.validate()
