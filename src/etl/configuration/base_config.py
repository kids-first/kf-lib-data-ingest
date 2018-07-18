from abc import (
    ABC,
    abstractmethod
)


class AbstractConfig(ABC):

    def __init__(self, filepath, **kwargs):
        self.config_filepath = filepath
        self.contents = self._read_file(filepath)
        self._validate(**kwargs)

    @abstractmethod
    def _read_file(self, filepath):
        """Raises some custom exception on failed read"""
        pass

    @abstractmethod
    def _validate(self, **kwargs):
        """Raises some custom exception on failed validation"""
        pass


class YamlConfig(AbstractConfig):

    def _read_file(self, filepath):
        # TODO
        pass

    def _validate(self, **kwargs):
        # TODO
        pass


class PyModuleConfig(AbstractConfig):
    def _read_file(self, filepath):
        # TODO
        pass

    def _validate(self, **kwargs):
        # TODO
        pass
