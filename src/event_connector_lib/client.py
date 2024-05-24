import threading
import queue
import logging
import uuid
import enum
import traceback
import requests
import json
from typing import Callable

from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

from event_connector_lib.utils import Event


class ModuleType(enum.Enum):
    """
    Enumeration for different types of ZKMS Modules.

    Attributes:
    CORE (int): Represents a core module within the ZKMS System.
    SUPPORT (int): Represents a support module within the ZKMS System.
    AI (int): Represents an AI module within the ZKMS System.
    """

    CORE = 0
    SUPPORT = 1
    AI = 2


class Client:
    """
    Client class to manage incoming and outgoing events, handle registrations,
    and communicate with a broker.

    Methods:
        set_event_handler(receiver_func: Callable[[Event], None]) -> None:
            Registers a function to handle incoming events.

        connect_broker(host: str, port: int) -> None:
            Connects to a broker and sends a registration event.

        send_event(event: Event) -> None:
            Sends an outgoing event by adding it to the outgoing events queue.

        subscribe_topic(topic: str) -> None:
            Placeholder method for subscribing to a topic (not implemented).
    """

    __incoming_events_queue: queue.Queue[Event] = queue.Queue()
    __outgoing_events_queue: queue.Queue[Event] = queue.Queue()
    __response_registry = {}
    __receiver_func = None
    __http_server_thread = None

    def __init__(
        self,
        host: str,
        port: int,
        name: str,
        description: str,
        version: str,
        module_type: ModuleType,
    ) -> None:
        self.host = host
        self.port = port
        self.name = name
        self.description = description
        self.version = version
        self.module_type = module_type

        threading.Thread(target=self.__process_incoming_events, daemon=True).start()
        threading.Thread(target=self.__process_outgoing_events, daemon=True).start()

        self.__http_server_thread = threading.Thread(
            target=self.__start_listening, args=(host, port), daemon=True
        )
        self.__http_server_thread.start()

    #
    # Event Queue Management
    #

    def __process_incoming_events(self) -> None:
        while True:
            event = self.__incoming_events_queue.get()
            logging.info("[INCOMING EVENT QUEUE] Process: %s", event.get_topic())

            if self.__receiver_func is None:
                logging.warn(
                    "Received an event, but no event handler was registered. Discarding event…"
                )
                continue

            self.__receiver_func(event)

    def __process_outgoing_events(self) -> None:
        while True:
            event = self.__outgoing_events_queue.get()
            try:
                # if event.is_response_requested() is True:
                #     __register_topic_response_handler(
                #         response_topic=event.get_reponse_topic(),
                #         topic=event.get_topic(),
                #     )

                requests.post(
                    url=f"http://{self.host}:{self.port}/event",
                    json=event.data,
                    timeout=60,
                )
                logging.info(
                    "Forwarded Event from Outgoing Queue: %s", event.get_topic()
                )
            except (
                requests.RequestException,
                requests.ConnectionError,
                requests.ConnectTimeout,
                requests.HTTPError,
            ):
                traceback.print_exc()
                logging.error("Error Forwarding Event: %s", event.get_topic())

    def __put_incoming_event_into_queue(self, event) -> None:
        if not isinstance(event, Event):
            return
        self.__incoming_events_queue.put(event)
        logging.info("Put Incoming Event in Queue: %s", event.get_topic())

    def __put_outgoing_event_into_queue(self, event: Event) -> None:
        if not isinstance(event, Event):
            return
        self.__outgoing_events_queue.put(event)
        logging.info("Put Event in Outgoing Queue: %s", event.get_topic())

    def __start_listening(self, host: str, port: int) -> None:
        httpd = HTTPServer((host, port), HTTPRequestHandler)
        httpd.serve_forever()

    #
    # Public API
    #

    def set_event_handler(self, receiver_func: Callable[[Event], None]) -> None:
        self.__receiver_func = receiver_func

    def connect_broker(self, host: str, port: int) -> None:
        self.__broker_host = host
        self.__broker_port = port

        event_data = {
            "event": {
                "topic": "/zkms/register/module",
                "respond_to": f"{str(uuid.uuid4())}-ResponseEvent-for-/zkms/register/module",
                "response_requested": False,
            },
            "payload": {
                "registration": {
                    "name": f"{self.name}",
                    "description": f"{self.description}",
                    "version": f"{self.version}",
                    "type": f"{self.module_type}",
                    "eventHandler": f"http://{self.host}:{self.port}",
                    "topics": ["*"],
                }
            },
        }

        event = Event(data=event_data)
        self.__put_outgoing_event_into_queue(event)

    def send_event(self, event: Event) -> None:
        self.__put_outgoing_event_into_queue(event)

    def subscribe_topic(self, topic: str) -> None:
        pass

    def loop_forever(self):
        if self.__http_server_thread is None:
            raise RuntimeError("HTTP server thread has not been started.")

        try:
            self.__http_server_thread.join()
        except KeyboardInterrupt:
            logging.info("Shutting down the client.")

    #
    # Util
    #

    def __str__(self) -> str:
        return (
            f"Client(name={self.name}, description={self.description}, "
            f"version={self.version}, module_type={self.module_type})"
        )


class HTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        parsed_path = urlparse(self.path)
        if parsed_path.path == "/event":
            content_length = int(self.headers["Content-Length"])
            post_data = self.rfile.read(content_length)
            try:
                json_data = json.loads(post_data)
            except json.JSONDecodeError:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'{"msg": "Invalid JSON"}')
                return

            event = json_data.get("event", None)
            payload = json_data.get("payload", None)
            if event is None or payload is None:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'{"msg": "Missing event or payload"}')
                return

            topic = event.get("topic")
            if topic is None:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'{"msg": "Missing topic"}')
                return

            # Put the incoming event into the queue
            # self.server.put_incoming_event_into_queue(Event(data=json_data))

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"msg": "Not Found"}')


#
# Response Handling
#


# def __register_topic_response_handler(response_topic, topic):
#     if response_topic in response_registry:
#         logging.error(
#             "Response Topic %s has already been registered for {%s}.",
#             response_topic,
#             response_registry.get(response_topic),
#         )
#         return

#     response_registry[response_topic] = topic

#     topic_registration_event_data = {
#         "event": {
#             "topic": "/zkms/register/topic",
#             "respond_to": f"{str(uuid.uuid4())}-ResponseEvent-for-/zkms/register/topic",
#             "response_requested": False,
#         },
#         "payload": {
#             "eventHandler": f"{EVENT_HANDLER}",
#             "topics": [f"{response_topic}"],
#         },
#     }
#     topic_registration_event = Event(data=topic_registration_event_data)
#     __put_outgoing_event_into_queue(topic_registration_event)

#     logging.info(
#         "Successfully registered Response Topic %s for %s.",
#         response_topic,
#         response_registry.get(response_topic),
#     )


# def __unregister_topic_response_handler(response_topic):
#     if response_topic not in response_registry:
#         logging.error("Response Topic %s has not been registered.", response_topic)
#         return

#     topic_deregistration_event_data = {
#         "event": {
#             "topic": "/zkms/deregister/topic",
#             "respond_to": f"{str(uuid.uuid4())}-ResponseEvent-for-/zkms/deregister/topic",
#             "response_requested": False,
#         },
#         "payload": {
#             "eventHandler": f"{EVENT_HANDLER}",
#             "topics": [f"{response_topic}"],
#         },
#     }
#     topic_deregistration_event = Event(data=topic_deregistration_event_data)
#     __put_outgoing_event_into_queue(topic_deregistration_event)

#     response_registry.pop(response_topic)
#     logging.info("Successfully unregistered Response Topic %s.", response_topic)


# def __is_response_topic_registered(response_topic):
#     if response_topic in response_registry:
#         return True

#     return False
