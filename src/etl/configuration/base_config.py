import os
import jsonschema
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
        if not os.path.isfile(filepath):
            raise FileNotFoundError('File {} not found'.format(filepath))
        self.contents = self._read_file(filepath)
        self._validate(**kwargs)

    @abstractmethod
    def _read_file(self, filepath):
        """ Should raise a FileNotFoundError exception if file not exists """
        pass

    @abstractmethod
    def _validate(self, *args, **kwargs):
        """ Should raise appropriate exception on failed validation """
        pass


class YamlConfig(AbstractConfig):

    def _read_file(self, filepath):
        return read_yaml(filepath)

    def _validate(self, schema_path=None):
        schema = read_yaml(schema_path)
        jsonschema.validate(self.contents, schema)


class PyModuleConfig(AbstractConfig):

    def _read_file(self, filepath):
        return import_module_from_file(filepath)

    def _validate(self, **kwargs):
        # TODO
        pass
