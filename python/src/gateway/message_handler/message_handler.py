from common import message_protocol


class MessageHandler:
    def __init__(self, client_id):
        self.client_id = client_id

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(self.client_id, [fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(self.client_id, [])

    def deserialize_result_message(self, message):
        client_id, data = message_protocol.internal.deserialize(message)
        return client_id, data