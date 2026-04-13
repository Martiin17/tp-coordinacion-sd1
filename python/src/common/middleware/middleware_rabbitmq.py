import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class _MessageHandler:
    def __init__(self, ch, method):
        self._ch = ch
        self._method = method

    def ack(self):
        self._ch.basic_ack(delivery_tag=self._method.delivery_tag)

    def nack(self):
        self._ch.basic_nack(delivery_tag=self._method.delivery_tag, requeue=True)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self._channel = self._connection.channel()
        self._queue_name = queue_name
        self._channel.queue_declare(queue=queue_name, durable=True)

    def send(self, message):
        self._channel.basic_publish(
            exchange='',
            routing_key=self._queue_name,
            body=message
        )

    def start_consuming(self, on_message_callback):
        self._channel.basic_qos(prefetch_count=1)
        self._on_message_callback = on_message_callback
        self._channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=self._handle_message,
            auto_ack=False
        )
        self._channel.start_consuming()
        self.close()

    def _handle_message(self, ch, method, properties, body):
        handler = _MessageHandler(ch, method)
        self._on_message_callback(body, handler.ack, handler.nack)

    def stop_consuming(self):
        self._channel.stop_consuming()

    def close(self):
        if not self._connection.is_closed:
            self._connection.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self._channel = self._connection.channel()
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='direct'
        )
        result = self._channel.queue_declare(queue='', exclusive=True)
        self._queue_name = result.method.queue
        for routing_key in routing_keys:
            self._channel.queue_bind(
                exchange=exchange_name,
                queue=self._queue_name,
                routing_key=routing_key
            )

    def send(self, message):
        self._channel.basic_publish(
            exchange=self._exchange_name,
            routing_key=self._routing_keys[0],
            body=message
        )

    def start_consuming(self, on_message_callback):
        self._channel.basic_qos(prefetch_count=1)
        self._on_message_callback = on_message_callback
        self._channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=self._handle_message,
            auto_ack=False
        )
        self._channel.start_consuming()
        self.close()

    def _handle_message(self, ch, method, properties, body):
        handler = _MessageHandler(ch, method)
        self._on_message_callback(body, handler.ack, handler.nack)

    def stop_consuming(self):
        self._channel.stop_consuming()

    def close(self):
        if not self._connection.is_closed:
            self._connection.close()