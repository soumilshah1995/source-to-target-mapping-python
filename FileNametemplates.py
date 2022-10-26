import datetime
from ast import literal_eval
import uuid

data = [
    {
        "mask_name":"YYYY",
        "mask_value":"datetime.datetime.now().year",
        "mask_type":1
    },
    {
        "mask_name":"MM",
        "mask_value":"datetime.datetime.now().month",
        "mask_type":1
    },
    {
        "mask_name":"DD",
        "mask_value":"datetime.datetime.now().day",
        "mask_type":1
    },
    {
        "mask_name":"GUID",
        "mask_value":"uuid.uuid4().__str__()",
        "mask_type":1
    },
    {
        "mask_name":"MYVAR",
        "mask_value":"SOUMIL",
        "mask_type":2
    }
]


filename = "YYYY_MYVAR_GUID.json"

my_variable = ""

for row, item in enumerate(data):

    if item.get("mask_name") in filename:

        filename = filename.strip().replace(" ", "")

        if item.get("mask_type") == 1 :
            filename = filename.replace(
                item.get("mask_name").strip(),
                str(eval(item.get("mask_value"))))

        if item.get("mask_type") ==2 :
            filename = filename.replace(item.get("mask_name").strip(),
                                        item.get("mask_value"))

print(filename)
