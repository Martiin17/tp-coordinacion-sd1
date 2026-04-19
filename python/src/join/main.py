import os
import logging
import bisect
from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops = {}
        self.partial_count = {}

    def process_message(self, message, ack, nack):
        logging.info("Received partial top")
        client_id, data = message_protocol.internal.deserialize(message)
        logging.info(f"Received partial top: {data}")

        current_top = self.partial_tops.setdefault(client_id, [])
        for fruit, amount in data:
            bisect.insort(current_top, fruit_item.FruitItem(fruit, amount))

        count = self.partial_count.get(client_id, 0) + 1
        self.partial_count[client_id] = count
        logging.info(f"Partial top {count}/{AGGREGATION_AMOUNT} for client {client_id}")

        if count == AGGREGATION_AMOUNT:
            full_top = self.partial_tops.pop(client_id)
            del self.partial_count[client_id]

            fruit_chunk = list(full_top[-TOP_SIZE:])
            fruit_chunk.reverse()
            top = list(map(lambda fi: (fi.fruit, fi.amount), fruit_chunk))

            logging.info(f"Sending final top for client {client_id}")
            logging.info(f"Final top: {top}")
            self.output_queue.send(message_protocol.internal.serialize(client_id, top))

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
