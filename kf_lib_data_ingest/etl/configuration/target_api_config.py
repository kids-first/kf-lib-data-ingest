"""
Configuration module specifying how a target model maps to the standard model.

Your target API configuration module must contain a list named `all_targets`
which contains, in the order that you want them to be loaded into the target
service, target entity builder classes (not instances) of the form:

    class Foo:
        class_name = 'foo'
        target_id_concept = CONCEPT.FOO.TARGET_SERVICE_ID

        @staticmethod
        def transform_records_list(records_list):
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

        @staticmethod
        def build_key(record):
            '''
            Composes a string that can uniquely identify the given record.

            :param record: CONCEPT values representing one record of extracted data
            :type record: dict
            :return: string of record components that uniquely identify the entity
                in the dataset
            :rtype: str
            :raises: Exception if record is not valid for entity
            '''
            return unique_key_composed_from_record

        @staticmethod
        def build_entity(record, key, get_target_id_from_record):
            '''
            Constructs a payload body that can be submitted to the target service
            for the given record.

            :param record: CONCEPT values representing one record of extracted data
            :type record: dict
            :param key: the value returned by the build_key method
            :type key: str
            :param get_target_id_from_record: a function that, given input arguments
                (entity_class, record), will return the unique reference identifier
                assigned by the target service for that entity
            :type get_target_id_from_record: function
            :return: an entity body ready to send to the target service
            '''
            return payload_body_composed_from_record

The all_targets list will look like this:

    all_targets = [
        Foo,
        ...
    ]

Your target API configuration module must also contain a `submit` function with
the following signature that sends a ready entity payload to the target service
and returns the unique identifier of the target entity that was created or
updated on the target server:

    def submit(host, entity_class, body):
        '''
        Negotiate submitting the data for an entity to the target service.

        :param host: host url
        :type host: str
        :param entity_class: which entity class is being sent
        :type entity_class: class
        :param body: entity body constructed by entity_class.build_entity
        :return: The target entity reference ID that the service says was
            created or updated
        :rtype: str
        :raise: RequestException on error
        '''
        ...

Your entity classes and submit function can do anything else you want as long
as they meet those minimum requirements.
"""

from collections import defaultdict
from inspect import isclass, isfunction, signature
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
                    invalid_targets["no build_entity"].append(t)
                elif not isfunction(t.build_entity):
                    invalid_targets["build_entity not a method"].append(t)
                elif not (
                    list(signature(t.build_entity).parameters)
                    == ["record", "key", "get_target_id_from_record"]
                ):
                    invalid_targets[
                        "build_entity takes wrong input arguments"
                    ].append(t)

                if not hasattr(t, "build_key"):
                    invalid_targets["no build_key"].append(t)
                elif not isfunction(t.build_key):
                    invalid_targets["build_key not a method"].append(t)
                elif not (
                    list(signature(t.build_key).parameters) == ["record"]
                ):
                    invalid_targets[
                        "build_key takes wrong input arguments"
                    ].append(t)

                if hasattr(t, "transform_records_list"):
                    if not isfunction(t.transform_records_list):
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
                "   class_name = 'foo'\n"
                "   target_id_concept = CONCEPT.FOO.TARGET_SERVICE_ID\n"
                "\n"
                "   @staticmethod\n"
                "   def transform_records_list(records_list):\n"
                "       # this method is optional\n"
                "       return new_records_list\n"
                "\n"
                "   @staticmethod\n"
                "   def build_key(record):\n"
                "       return unique_key_composed_from_record\n"
                "\n"
                "   @staticmethod\n"
                "   def build_entity(record, key, target_id_lookup_func):\n"
                "       return payload_body_composed_from_record\n"
                "\n\n"
                f"Errors found:\n {pformat(dict(invalid_targets))}"
            )

        if not (
            isfunction(self.submit)
            and (
                list(signature(self.submit).parameters)
                == ["host", "entity_class", "body"]
            )
        ):
            raise ConfigValidationError(
                f"Target API Config module {self.config_filepath} is missing"
                " required function signature `submit(host, entity_class, body)`"
                " that sends an entity to the target service and returns the"
                " designated unique target service identifier."
            )

        if isfunction(self.validate):
            self.validate()
