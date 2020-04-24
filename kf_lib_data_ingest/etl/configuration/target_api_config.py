"""
Configuration module specifying how a target model maps to the standard model.

Your target API configuration module must contain a list variable named
`all_targets` which contains, in the order that you want them to be loaded into
the target service, target entity classes (not instances) of the form:

    class Foo:
        @staticmethod
        def build_key(row):
            '''
            :param row: CONCEPT values representing one row of extracted data
            :type row: dict
            :return: string of row components that uniquely identify the entity
                in the dataset
            :rtype: str
            :raises: Exception if row is not valid for entity
            '''
            return unique_key_composed_from_row

        @staticmethod
        def build_entity(row, key, target_id_lookup_func):
            '''
            :param row: CONCEPT values representing one row of extracted data
            :type row: dict
            :param key: the value returned by the build_key method
            :type key: str
            :param target_id_lookup_func: a function that, given input arguments
                (entity_class.__name__, entity_class.build_key(row)), will return
                the unique reference identifier assigned by the target service
                for that entity
            :type target_id_lookup_func: function
            :return: an entity body ready to send to the target service
            '''
            return payload_body_composed_from_row

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
            elif not hasattr(t, "build_entity"):
                invalid_targets["no build_entity"].append(t)
            elif not isfunction(t.build_entity):
                invalid_targets["build_entity not a method"].append(t)
            elif not (
                list(signature(t.build_entity).parameters)
                == ["row", "key", "target_id_lookup_func"]
            ):
                invalid_targets[
                    "build_entity takes wrong input arguments"
                ].append(t)
            elif not hasattr(t, "build_key"):
                invalid_targets["no build_key"].append(t)
            elif not isfunction(t.build_key):
                invalid_targets["build_key not a method"].append(t)
            elif not (list(signature(t.build_key).parameters) == ["row"]):
                invalid_targets["build_key takes wrong input arguments"].append(
                    t
                )

        if invalid_targets:
            raise ConfigValidationError(
                "The all_targets list must contain only classes of the form:\n"
                "\n"
                "class Foo:\n"
                "   @staticmethod\n"
                "   def build_key(row):\n"
                "       return unique_key_composed_from_row\n"
                "\n"
                "   @staticmethod\n"
                "   def build_entity(row, key, target_id_lookup_func):\n"
                "       return payload_body_composed_from_row\n"
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
