import os
import logging
from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

SUM_CONTROL_PREFIX = "SUM_CONTROL_QUEUE"


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.input_queue.add_queue(
            f"{SUM_CONTROL_PREFIX}_{ID}", self.process_control_message
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            exchange = middleware.MessageMiddlewareExchangeProducerRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX
            )
            self.data_output_exchanges.append((exchange, f"{AGGREGATION_PREFIX}_{i}"))

        self.control_outputs = []
        for i in range(SUM_AMOUNT):
            control_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_CONTROL_PREFIX}_{i}"
            )
            self.control_outputs.append(control_queue)

        self.amount_by_client_fruit = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Process data")
        key = (client_id, fruit)
        self.amount_by_client_fruit[key] = self.amount_by_client_fruit.get(
            key, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Sending data messages for client {client_id}")
        keys_to_send = [k for k in self.amount_by_client_fruit if k[0] == client_id]
        items_to_send = [(k, self.amount_by_client_fruit[k]) for k in keys_to_send]

        for _, final_fruit_item in items_to_send:
            for exchange, r_key in self.data_output_exchanges:
                exchange.send(
                    message_protocol.internal.serialize(
                        client_id, [final_fruit_item.fruit, final_fruit_item.amount]
                    ),
                    routing_key=r_key
                )

        logging.info(f"Sending EOF for client {client_id}")
        for exchange, r_key in self.data_output_exchanges:
            exchange.send(
                message_protocol.internal.serialize(client_id, []),
                routing_key=r_key
            )

        for k in keys_to_send:
            del self.amount_by_client_fruit[k]

    def process_data_message(self, message, ack, nack):
        client_id, data = message_protocol.internal.deserialize(message)
        if len(data) == 2:
            self._process_data(client_id, *data)
            ack()
        else:
            for control_queue in self.control_outputs:
                control_queue.send(
                    message_protocol.internal.serialize(client_id, [])
                )
            ack()

    def process_control_message(self, message, ack, nack):
        client_id, _ = message_protocol.internal.deserialize(message)
        self._process_eof(client_id)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_message)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
