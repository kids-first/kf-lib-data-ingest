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


class Good:
    @staticmethod
    def build_key(row):
        pass

    @staticmethod
    def build_entity(row, key, target_id_lookup_func):
        pass


all_targets = []


def submit(host, entity_class, body):
    pass
