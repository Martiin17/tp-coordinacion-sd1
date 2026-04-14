import os
import logging
import socket
import signal
import multiprocessing
import message_handler
from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


def handle_client_request(client_socket, message_handler_instance):
    output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    try:
        while True:
            message = message_protocol.external.recv_msg(client_socket)

            if message[0] == message_protocol.external.MsgType.FRUIT_RECORD:
                serialized_message = message_handler_instance.serialize_data_message(message[1])
                output_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )

            if message[0] == message_protocol.external.MsgType.END_OF_RECODS:
                serialized_message = message_handler_instance.serialize_eof_message(message[1])
                output_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
                return
    except socket.error:
        logging.error("The connection with the client was lost")
    except Exception as e:
        logging.error(e)
    finally:
        output_queue.close()


def handle_client_response(client_list):
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _consume_result(message, ack, nack):
        try:
            client_id, data = message_handler.MessageHandler(None).deserialize_result_message(message)

            if not data:
                ack()
                return

            for entry in client_list:
                if entry[0] == client_id:
                    client_socket = entry[1]
                    try:
                        message_protocol.external.send_msg(
                            client_socket,
                            message_protocol.external.MsgType.FRUIT_TOP,
                            data,
                        )
                        message_protocol.external.recv_msg(client_socket)
                    except socket.error:
                        logging.error(f"Connection lost with client {client_id}")
                    finally:
                        client_list.remove(entry)
                    break

            ack()
        except Exception as e:
            logging.error(e)
            nack()
            input_queue.stop_consuming()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    next_client_id = multiprocessing.Value("i", 0)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)

        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()
                        logging.info("A new client has connected")

                        with next_client_id.get_lock():
                            client_id = next_client_id.value
                            next_client_id.value += 1

                        message_handler_instance = message_handler.MessageHandler(client_id)
                        client_list.append([client_id, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()
