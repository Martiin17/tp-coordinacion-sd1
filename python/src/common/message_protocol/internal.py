import json


def serialize(client_id, data):
    return json.dumps({"client_id": client_id, "data": data}).encode("utf-8")


def deserialize(message):
    parsed = json.loads(message.decode("utf-8"))
    return parsed["client_id"], parsed["data"]