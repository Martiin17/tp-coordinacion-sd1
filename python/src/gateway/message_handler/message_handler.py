import uuid
from common import message_protocol


class MessageHandler:
    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(self.client_id, [fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(self.client_id, [])

    def deserialize_result_message(self, message):
        res_client_id, data = message_protocol.internal.deserialize(message)
        if res_client_id == self.client_id:
            return data
        return None