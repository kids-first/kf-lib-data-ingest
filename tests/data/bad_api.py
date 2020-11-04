class A:
    pass


class B:
    @classmethod
    def get_key_components(cls):
        pass


class C:
    @classmethod
    def build_entity(cls):
        pass


class D:
    @classmethod
    def get_key_components(cls):
        pass

    @classmethod
    def build_entity(cls):
        pass


class E:
    @classmethod
    def transform_records_list(cls):
        pass

    @classmethod
    def get_key_components(cls):
        pass

    @classmethod
    def build_entity(cls):
        pass


class F:
    class_name = "bad"
    target_id_concept = "CONCEPT|FOO|TARGET_SERVICE_ID"

    @classmethod
    def transform_records_list(cls, records_list):
        pass

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        pass

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        pass


class Good:
    class_name = "good"
    target_id_concept = "CONCEPT|FOO|TARGET_SERVICE_ID"

    @classmethod
    def transform_records_list(cls, records_list):
        pass

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        pass

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        pass

    @classmethod
    def submit(cls, host, body):
        pass


all_targets = []


def submit(host, entity_class, body):
    pass
