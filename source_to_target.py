from jsonpath_ng import parse

"""Items which will be stored in database """
json_data = {
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
            "destination_field_type": "str",
            "default_value": -1
        }
    ],
    "mapping": [
        {
            "mapping_id": 1,
            "mapping_source": 1,
            "mapping_destination": 1
        }
    ]
}

json_incoming_data = {
    "id":"12213"
}


class Source(object):

    def __init__(self):
        pass

    def get_data_field(self, field_name):
        data = self.fetch_data()
        for item in data:
            for key, value in item.items():
                if key == field_name:
                    return item
        return None

    def fetch_data(self):
        return json_data.get("source")

    def get_data_id(self, id):
        self.id = id
        data = self.fetch_data()
        for x in data:
            if x.get("id") == self.id:
                return x
        return None


class Target(object):
    def __init__(self):
        pass

    def get_data_field(self, field_name):
        data = self.fetch_data()
        for item in data:
            for key, value in item.items():
                if key == field_name:
                    return item
        return None

    def fetch_data(self):
        return json_data.get("destination")

    def get_data_id(self, id):
        self.id = id
        data = self.fetch_data()
        for x in data:
            if x.get("id").__str__() == self.id.__str__():
                return x
        return None


class Mappings(object):

    def __init__(self):
        pass

    def fetch_data(self):
        return json_data.get("mapping")


mapping_instance = Mappings()
mapping_data = mapping_instance.fetch_data()

source_instance = Source()
destination_instance = Target()

transformed_data = []
json_data_transformed = {}

for mappings in mapping_data:

    """fetch the source mapping """
    mapping_source_id = mappings.get("mapping_source")
    mapping_destination_id = mappings.get("mapping_destination")

    mapping_source_data = source_instance.get_data_id(mapping_source_id)

    """Fetch Source  field Name"""
    source_field_name = mapping_source_data.get("source_field_name")

    if source_field_name not in json_incoming_data.keys():
        if mapping_source_data.get("is_required"):
            print("Alert ! Field {} is not present in JSON please FIX mappings ".format(source_field_name))
            raise Exception("Alert ! Field {} is not present in JSON please FIX mappings ".format(source_field_name))

    else:
        source_json_path = mapping_source_data.get("source_field_mapping")

        """Query the JSON based on PATh"""
        jsonpath_expression = parse(source_json_path)

        match = jsonpath_expression.find(json_incoming_data)
        source_data_value = match[0].value

        """check the data type for source if matches with what we have previous """
        if mapping_source_data.get("source_field_type") != type(source_data_value).__name__:

            if source_data_value is not None:
                _message = ("Alert ! Source Field :{} Datatype has changed from {} to {} ".format(source_field_name,
                                                                                                  mapping_source_data.get(
                                                                                                      "source_field_type"),
                                                                                                  type(
                                                                                                      source_data_value).__name__
                                                                                                  ))
                print(_message)
                raise Exception(_message)

        """Query and fetch the Destination | target """
        destination_mappings_json_object = destination_instance.get_data_id(mapping_destination_id)
        destination_field_name = destination_mappings_json_object.get("destination_field_name")

        destination_field_type = destination_mappings_json_object.get("destination_field_type")

        """Datatype Conversion """
        if destination_field_type == "str":

            """is source is none iinsert default value"""
            if source_data_value is None:
                json_data_transformed[destination_field_name] = destination_mappings_json_object.get("default_value")
                transformed_data.append(json_data_transformed)
            else:
                json_data_transformed[destination_field_name] = source_data_value
                transformed_data.append(json_data_transformed)

        if destination_field_type == "float":
            if source_data_value is None:
                json_data_transformed[destination_field_name] = destination_mappings_json_object.get("default_value")
                transformed_data.append(json_data_transformed)
            else:
                json_data_transformed[destination_field_name] = float(source_data_value)
                transformed_data.append(json_data_transformed)

        if destination_field_type == "int":
            if source_data_value is None:
                json_data_transformed[destination_field_name] = destination_mappings_json_object.get("default_value")
                transformed_data.append(json_data_transformed)
            else:
                json_data_transformed[destination_field_name] = int(source_data_value)
                transformed_data.append(json_data_transformed)

        if destination_field_type == "list":
            if source_data_value is None:
                json_data_transformed[destination_field_name] = destination_mappings_json_object.get("default_value")
                transformed_data.append(json_data_transformed)
            else:
                json_data_transformed[destination_field_name] = list(source_data_value)
                transformed_data.append(json_data_transformed)

        if destination_field_type == "set":
            if source_data_value is None:
                json_data_transformed[destination_field_name] = destination_mappings_json_object.get("default_value")
                transformed_data.append(json_data_transformed)
            else:
                json_data_transformed[destination_field_name] = set(source_data_value)
                transformed_data.append(json_data_transformed)

        if destination_field_type == "bool":
            if source_data_value is None:
                json_data_transformed[destination_field_name] = destination_mappings_json_object.get("default_value")
                transformed_data.append(json_data_transformed)
            else:
                json_data_transformed[destination_field_name] = bool(source_data_value)
                transformed_data.append(json_data_transformed)

        if destination_field_type == "bytes":
            if source_data_value is None:
                json_data_transformed[destination_field_name] = destination_mappings_json_object.get("default_value")
                transformed_data.append(json_data_transformed)
            else:
                json_data_transformed[destination_field_name] = bytes(source_data_value)
                transformed_data.append(json_data_transformed)

print(f"""

    ====================SOURCE=====================
                {json_incoming_data}
    ====================Transformed================
             {transformed_data[0]}
    =================================================
    
""")
