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
    def build_entity(row, key, get_target_id_from_row):
        pass


all_targets = []


def submit(host, entity_class, body):
    pass
