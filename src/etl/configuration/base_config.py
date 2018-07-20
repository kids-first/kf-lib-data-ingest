import os
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from abc import (
    ABC,
    abstractmethod
)

from common.misc import (
    import_module_from_file,
    read_yaml
)


class AbstractConfig(ABC):

    def __init__(self, filepath, **kwargs):
        self.config_filepath = filepath
        self.contents = self._read_file(filepath)
        self._validate(**kwargs)

    @abstractmethod
    def _read_file(self, filepath):
        """ Should raise a FileNotFoundError exception if file not exists """
        pass

    @abstractmethod
    def _validate(self, **kwargs):
        """ Should raise appropriate exception on failed validation """
        pass


class YamlConfig(AbstractConfig):

    def _read_file(self, filepath):
        if not os.path.isfile(filepath):
            raise FileNotFoundError('Yaml config file {} not found'
                                    .format(filepath))
        return read_yaml(filepath)

    def _validate(self, schema=None):
        if not schema:
            raise Exception('Cannot validate yaml file! No schema provided')
        validate(self.config_filepath, schema)


class PyModuleConfig(AbstractConfig):

    def _read_file(self, filepath):
        return import_module_from_file(filepath)

    def _validate(self, **kwargs):
        # TODO
        pass
