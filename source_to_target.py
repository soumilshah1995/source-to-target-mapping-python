from abc import ABC, abstractmethod

from jsonpath_ng import parse


class Interface(ABC):

    @abstractmethod
    def get_data_by_field(self, field_name):
        """Fetch the data by given feild name """

    @abstractmethod
    def get_data_by_id(self, id):
        """Fetch the data by given ID  """

    @abstractmethod
    def get(self):
        """Fetch all data """


class Database(object):
    def __init__(self):
        pass

    @property
    def get_data_source_target_mapping(self):
        return {
            "source": [
                {
                    "id": 1,
                    "source_field_name": "id",
                    "source_field_mapping": "$.id",
                    "source_field_type": "str",
                    "is_required": True
                }
            ],
            "destination": [
                {
                    "id": 1,
                    "destination_field_name": "customerID",
                    "destination_field_mapping": "customerID",
                    "destination_field_type": "int",
                    "default_value": -1
                }
            ],
            "mapping": [
                {
                    "id": 1,
                    "mapping_source": 1,
                    "mapping_destination": 1
                }
            ]
        }


class Source(Interface, Database):
    def __init__(self):
        Database.__init__(self)

    def get_data_by_field(self, field_name):
        data = self.get
        for item in data:
            for key, value in item.items():
                if key == field_name:
                    return item
        return None

    @property
    def get(self):
        return self.get_data_source_target_mapping.get("source")

    def get_data_by_id(self, id):
        self.id = id
        data = self.get
        for x in data:
            if x.get("id") == self.id:
                return x
        return None


class Target(Interface, Database):

    def __init__(self):
        Database.__init__(self)

    def get_data_by_field(self, field_name):
        data = self.get
        for item in data:
            for key, value in item.items():
                if key == field_name:
                    return item
        return None

    @property
    def get(self):
        return self.get_data_source_target_mapping.get("destination")

    def get_data_by_id(self, id):
        self.id = id
        data = self.get
        for x in data:
            if x.get("id").__str__() == self.id.__str__():
                return x
        return None


class Mappings(Interface, Database):

    def __init__(self):
        Database.__init__(self)

    @property
    def get(self):
        return self.get_data_source_target_mapping.get("mapping")

    def get_data_by_id(self, id):
        self.id = id
        data = self.get
        for x in data:
            if x.get("id").__str__() == self.id.__str__():
                return x
        return None

    def get_data_by_field(self, field_name):
        return None


class JsonQuery(object):

    def __init__(self, json_path, json_data):
        self.json_path = json_path
        self.json_data = json_data

    def get(self):
        jsonpath_expression = parse(self.json_path)
        match = jsonpath_expression.find(self.json_data)
        source_data_value = match[0].value
        return source_data_value


class Transform(object):
    def __init__(self, input_json):

        self.input_json = input_json

        self.mapping_instance = Mappings()
        self.source_instance = Source()
        self.destination_instance = Target()
        self.json_data_transformed = {}

    def _get_mapping_data(self):
        return self.mapping_instance.get

    def _get_mapping_source_data(self):
        return self.source_instance.get

    def get_transformed_data(self):

        for mappings in self._get_mapping_data():

            """fetch the source mapping """
            mapping_source_id = mappings.get("mapping_source")
            mapping_destination_id = mappings.get("mapping_destination")

            mapping_source_data = self.source_instance.get_data_by_id(id=mapping_source_id)

            """Fetch Source  field Name"""
            source_field_name = mapping_source_data.get("source_field_name")

            """if field given is not present incoming json """
            if source_field_name not in self.input_json.keys():
                if mapping_source_data.get("is_required"):
                    print("Alert ! Field {} is not present in JSON please FIX mappings ".format(source_field_name))
                    raise Exception(
                        "Alert ! Field {} is not present in JSON please FIX mappings ".format(source_field_name))
            else:
                source_data_value = JsonQuery(
                    json_path=mapping_source_data.get("source_field_mapping"),
                    json_data=self.input_json
                ).get()

                """check the data type for source if matches with what we have previous """
                if mapping_source_data.get("source_field_type") != type(source_data_value).__name__:
                    if source_data_value is not None:
                        _message = (
                            "Alert ! Source Field :{} Datatype has changed from {} to {} ".format(source_field_name,
                                                                                                  mapping_source_data.get(
                                                                                                      "source_field_type"),
                                                                                                  type(
                                                                                                      source_data_value).__name__))
                        print(_message)
                        raise Exception(_message)

                """Query and fetch the Destination | target """
                destination_mappings_json_object = self.destination_instance.get_data_by_id(
                    id=mappings.get("mapping_destination"))
                destination_field_name = destination_mappings_json_object.get("destination_field_name")
                destination_field_type = destination_mappings_json_object.get("destination_field_type")

                dtypes = [str, float, list, int, set]


                for dtype in dtypes:

                    """Datatype Conversion """
                    if destination_field_type == str(dtype.__name__):
                        print("dtype", str(dtype.__name__))

                        """is source is none insert default value"""
                        if source_data_value is None:
                            self.json_data_transformed[destination_field_name] = dtype.__call__(
                                destination_mappings_json_object.get("default_value")
                            )
                        else:
                            self.json_data_transformed[destination_field_name] = dtype.__call__(source_data_value)

        print(self.json_data_transformed)


def main():

    json_incoming_data = {
        "id": None
    }

    helper = Transform(input_json=json_incoming_data)
    response = helper.get_transformed_data()

    print(response)


main()
