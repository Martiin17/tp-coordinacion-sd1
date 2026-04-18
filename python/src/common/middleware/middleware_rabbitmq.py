import pika
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
        self._extra_queues = {}  # {queue_name: callback}

    def add_queue(self, queue_name, on_message_callback):
        self._channel.queue_declare(queue=queue_name, durable=True)
        self._extra_queues[queue_name] = on_message_callback

    def send(self, message, queue_name=None):
        target = queue_name if queue_name is not None else self._queue_name
        self._channel.basic_publish(
            exchange='',
            routing_key=target,
            body=message
        )

    def start_consuming(self, on_message_callback):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue=self._queue_name,
            on_message_callback=self._make_handler(on_message_callback),
            auto_ack=False
        )
        for queue_name, callback in self._extra_queues.items():
            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._make_handler(callback),
                auto_ack=False
            )
        self._channel.start_consuming()
        self.close()

    def _make_handler(self, callback):
        def handler(ch, method, properties, body):
            msg_handler = _MessageHandler(ch, method)
            callback(body, msg_handler.ack, msg_handler.nack)
        return handler

    def stop_consuming(self):
        self._channel.stop_consuming()

    def close(self):
        if not self._connection.is_closed:
            self._connection.close()


class MessageMiddlewareExchangeProducerRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, exchange_type="direct"):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self._channel = self._connection.channel()
        self._exchange_name = exchange_name
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
        )

    def send(self, message, routing_key=""):
        self._channel.basic_publish(
            exchange=self._exchange_name,
            routing_key=routing_key,
            body=message
        )

    def start_consuming(self, on_message_callback):
        raise NotImplementedError("El producer no consume.")

    def stop_consuming(self):
        pass

    def close(self):
        if not self._connection.is_closed:
            self._connection.close()


class MessageMiddlewareExchangeConsumerRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys, exchange_type="direct"):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self._channel = self._connection.channel()
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
        )
        result = self._channel.queue_declare(queue='', exclusive=True)
        self._queue_name = result.method.queue
        for routing_key in routing_keys:
            self._channel.queue_bind(
                exchange=exchange_name,
                queue=self._queue_name,
                routing_key=routing_key
            )

    def send(self, message, routing_key=""):
        raise NotImplementedError("Consumer don't send.")

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