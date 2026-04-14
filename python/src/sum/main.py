import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_client_fruit = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Process data")
        key = (client_id, fruit)
        self.amount_by_client_fruit[key] = self.amount_by_client_fruit.get(
            key, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages for client {client_id}")
        for key, final_fruit_item in self.amount_by_client_fruit.items():
            if key[0] != client_id:
                continue
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        client_id, [final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message for client {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize(client_id, [])
            )

        keys_to_delete = [k for k in self.amount_by_client_fruit if k[0] == client_id]
        for k in keys_to_delete:
            del self.amount_by_client_fruit[k]

    def process_data_message(self, message, ack, nack):
        client_id, data = message_protocol.internal.deserialize(message)
        if len(data) == 2:
            self._process_data(client_id, *data)
        else:
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
