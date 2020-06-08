class A:
    pass


class B:
    @staticmethod
    def build_key():
        pass


class C:
    @staticmethod
    def build_entity():
        pass


class D:
    @staticmethod
    def build_key():
        pass

    @staticmethod
    def build_entity():
        pass


class E:
    @staticmethod
    def transform_records_list():
        pass

    @staticmethod
    def build_key():
        pass

    @staticmethod
    def build_entity():
        pass


class Good:
    class_name = "good"
    target_id_concept = "CONCEPT|FOO|TARGET_SERVICE_ID"

    @staticmethod
    def transform_records_list(records_list):
        pass

    @staticmethod
    def build_key(record):
        pass

    @staticmethod
    def build_entity(record, key, get_target_id_from_record):
        pass


all_targets = []


def submit(host, entity_class, body):
    pass
