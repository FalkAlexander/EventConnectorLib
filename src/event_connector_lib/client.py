import threading
import queue
import logging
import uuid
import traceback
import requests
import json
from typing import Any, Callable, Dict, Optional

from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

from event_connector_lib.utils import BrokerEvent, Event, ModuleType


class ResponseCallbackError(Exception):
    """Raised when a response callback is provided but the event does not request a response."""


class AwaitEventResponseTimeout(TimeoutError):
    """Raised when an awaited event response is not received within a certain timeout."""


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
    __registered_response_callbacks: Dict[str, queue.Queue[Event]] = {}
    __receiver_func = None
    __http_server_thread = None
    __receiver_func_queue: queue.Queue[Event] = queue.Queue()

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
            logging.info("[INCOMING EVENT QUEUE] Process: %s", event.topic)

            if self.__receiver_func is None:
                logging.warn(
                    "Received an event, but no event handler was registered. Discarding event…"
                )
                continue

            if event.topic in self.__registered_response_callbacks:
                logging.debug(
                    "Received an event with a topic that has been registered for a response callback."
                )
                self.__registered_response_callbacks[event.topic].put(event)
                continue

            self.__receiver_func_queue.put(event)

    def __process_outgoing_events(self) -> None:
        while True:
            event = self.__outgoing_events_queue.get()
            try:
                # if event.is_response_requested() is True:
                #     __register_topic_response_handler(
                #         response_topic=event.get_reponse_topic(),
                #         topic=event.get_topic(),
                #     )

                if isinstance(event, BrokerEvent):
                    destination_url = event.destination
                else:
                    destination_url = (
                        f"http://{self.__broker_host}:{self.__broker_port}/event"
                    )

                requests.post(
                    url=destination_url,
                    json=event.get_raw_data(),
                    timeout=60,
                )
                logging.info("Forwarded Event from Outgoing Queue: %s", event.topic)
            except (
                requests.RequestException,
                requests.ConnectionError,
                requests.ConnectTimeout,
                requests.HTTPError,
            ):
                traceback.print_exc()
                logging.error("Error Forwarding Event: %s", event.topic)

    def _put_incoming_event_into_queue(self, event: Event) -> None:
        self.__incoming_events_queue.put(event)
        logging.info("Put Incoming Event in Queue: %s", event.topic)

    def _put_outgoing_event_into_queue(self, event: Event) -> None:
        self.__outgoing_events_queue.put(event)
        logging.info("Put Event in Outgoing Queue: %s", event.topic)

    def _subscribe_topics(self, topics: list[str]) -> None:
        topic_subscription_event_data = {
            "event": {
                "topic": "/zkms/register/topic",
                "response_requested": False,
            },
            "payload": {
                "eventHandler": f"http://{self.host}:{self.port}/event",
                "topics": topics,
            },
        }
        self.send_event(event=Event(data=topic_subscription_event_data))

    def _unsubscribe_topics(self, topics: list[str]) -> None:
        topic_unsubscription_event_data = {
            "event": {
                "topic": "/zkms/deregister/topic",
                "response_requested": False,
            },
            "payload": {
                "eventHandler": f"http://{self.host}:{self.port}/event",
                "topics": topics,
            },
        }
        self.send_event(event=Event(data=topic_unsubscription_event_data))

    def __start_listening(self, host: str, port: int) -> None:
        httpd = HTTPServer((host, port), self.__create_http_request_handler)
        httpd.serve_forever()

    def __create_http_request_handler(
        self, *args: Any, **kwargs: Any
    ) -> "_HTTPRequestHandler":
        return _HTTPRequestHandler(self, *args, **kwargs)

    def __await_event(
        self,
        topic: str,
        response_callback: Callable[[Event, Any], None],
        *args: Any,
        **kwargs: Any,
    ):
        if topic not in self.__registered_response_callbacks:
            self.__registered_response_callbacks[topic] = queue.Queue()

        response_queue = self.__registered_response_callbacks[topic]
        try:
            response_event = response_queue.get(timeout=30)
        except queue.Empty:
            logging.error(
                "Canceled awaiting response event with topic %s. Reason: Timeout reached.",
                topic,
            )
            return None
        except queue.Full:
            logging.error(
                "Canceled awaiting response event with topic %s. Reason: Queue full.",
                topic,
            )
            return None

        del self.__registered_response_callbacks[topic]

        response_callback(response_event, *args, **kwargs)

    #
    # Public API
    #

    def set_event_handler(self, receiver_func: Callable[[Event], None]) -> None:
        """
        Registers a function to handle incoming events.

        Args:
            receiver_func (Callable[[Event], None]): A function that takes an Event
                                                     as its parameter and handles it.

        Example:
            def my_event_handler(event: Event):
                print(f"Received event with topic: {event.get_topic()}")

            client.set_event_handler(my_event_handler)
        """
        self.__receiver_func = receiver_func

    def connect_broker(self, host: str, port: int) -> None:
        """
        Connects to a broker using the provided host and port, then sends a registration event.

        This method sets the broker's host and port for the client instance and creates an event data structure
        that includes registration details such as the name, description, version, type, and event handler URL
        for the module. It then encapsulates this data into an `Event` object and puts it into the outgoing
        events queue to be processed and sent to the broker.

        Args:
            host (str): The hostname or IP address of the broker.
            port (int): The port number on which the broker is listening.

        Example:
            client.connect_broker(host='127.0.0.1', port=10000)
        """
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
                    "eventHandler": f"http://{self.host}:{self.port}/event",
                    "topics": ["*"],
                }
            },
        }

        event = Event(data=event_data)
        self._put_outgoing_event_into_queue(event)

    def send_event(
        self,
        event: Event,
        response_callback: Optional[Callable[..., None]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Sends an event by placing it into the outgoing events queue.

        This method takes an `Event` object and adds it to the queue of outgoing events to be processed
        and sent to the broker. By calling this method, an event is effectively
        scheduled for delivery.

        Args:
            event (Event): The event object that contains the necessary data to be sent out.
            response_callback (Optional[Callable[..., None]]): A callback function that is invoked
            when a response event has been received. This callback function must accept
            the `Event` object as its first parameter, followed by additional arbitrary arguments.
            The response_callback is being called in the same thread from which send_event was called.
            *args (Any): Additional positional arguments to pass to the callback function.
            **kwargs (Any): Additional keyword arguments to pass to the callback function.

        The `response_callback` can be used to handle the response event as it is received.
        This mechanism is similar to a signal/slot or callback system, allowing you to define a
        function or method that will handle the response. The callback receives the response
        event and any additional data supplied, enabling you to perform necessary follow-up actions
        in response to the event.

        Example:
            event_data = {
                'event': {
                    'topic': 'sample/topic',
                    'respond_to': 'response/topic',
                    'response_requested': False
                },
                'payload': {
                    'key': 'value'
                }
            }
            event = Event(data=event_data)

            def handle_response(response_event: Event, extra_data1, extra_data2):
                print("Response received:", response_event)
                print("Additional data:", extra_data1, extra_data2)

            client.send_event(
                event,
                response_callback=handle_response,
                extra_arg1="extra1",
                extra_arg2="extra2"
            )
        """
        self._put_outgoing_event_into_queue(event)

        if response_callback is None:
            return

        if not event.response_requested:
            logging.warning(
                "Response_callback provided, but the event does not request a response."
            )
            raise ResponseCallbackError(
                "response_callback provided, but the event does not request a response."
            )
        if not event.response_topic:
            logging.warning(
                "Response_callback provided, but the event does not specify a response topic."
            )
            raise ResponseCallbackError(
                "response_callback provided, but the event does not specify a response topic."
            )
        if not callable(response_callback):
            raise TypeError("response_callback should be a callable function or None")

        # TODO: Execute __await_event in dedicated thread in order to not block send_event from here on
        self.__await_event(
            topic=event.response_topic,
            response_callback=response_callback,
            *args,
            **kwargs,
        )

    def send_event_and_await_response(self, event: Event, timeout: int = 60):
        """
        Sends an event and waits for a response with a specified timeout.

        This method sends the given event to the broker and then waits up to the specified number of seconds for a response. If no response is received within the timeout period, it raises an AwaitEventResponseTimeout exception.

        Args:
            event (Event): The event to send to the broker.
            timeout (int, optional): The maximum number of seconds to wait for a response before raising a timeout exception. Defaults to 60.

        Returns:
            Event: The response event received from the broker within the specified timeout period. If no response is received within the timeout period, an AwaitEventResponseTimeout exception will be raised instead.

        Raises:
            AwaitEventResponseTimeout: If a response is not received within the specified timeout period.

        Example:
            response = client.await_response_on_send_event(event=my_event, timeout=30)
        """
        response_received = threading.Event()
        response_event = None

        def handle_response(event: Event):
            nonlocal response_event
            response_event = event
            response_received.set()

        self.send_event(event=event, response_callback=handle_response)
        response_received.wait(timeout=timeout)

        if not response_received.is_set():
            raise AwaitEventResponseTimeout(
                "Timed out waiting for the awaited response event"
            )

        return response_event

    def subscribe_topic(self, topic: str) -> None:
        """
        Subscribes to a single topic.

        This method allows the client to subscribe to a specific topic, enabling it to
        receive and handle events published to that topic on the broker. By subscribing
        to a topic, the client can become a listener for messages related to the topic of interest.

        Args:
            topic (str): The topic string to subscribe to.

        Example:
            client.subscribe_topic("/sample/topic")
        """
        self._subscribe_topics([topic])

    def subscribe_topics(self, topics: list[str]) -> None:
        """
        Subscribes to multiple topics.

        This method allows the client to subscribe to a list of topics, enabling it to receive and handle
        events published to these topics on the broker. By subscribing to multiple topics, the client
        can become a listener for messages related to several topics of interest in a single call.

        Args:
            topics (list[str]): A list of topic strings to subscribe to.

        Example:
            client.subscribe_topics(["/sample/topic1", "/sample/topic2"])
        """
        self._subscribe_topics(topics)

    def unsubscribe_topic(self, topic: str) -> None:
        """
        Unsubscribes from a single topic.

        This method allows the client to unsubscribe from a specific topic, stopping it from
        receiving and handling events published to that topic on the broker. By unsubscribing
        from a topic, the client will no longer listen to messages related to the specified topic.

        Args:
            topic (str): The topic string to unsubscribe from.

        Example:
            client.unsubscribe_topic("/sample/topic")
        """
        self._unsubscribe_topics([topic])

    def unsubscribe_topics(self, topics: list[str]) -> None:
        """
        Unsubscribes from multiple topics.

        This method allows the client to unsubscribe from a list of topics, stopping it from
        receiving and handling events published to those topics on the broker. By unsubscribing
        from multiple topics, the client will no longer listen to messages related to the specified topics.

        Args:
            topics (list[str]): A list of topic strings to unsubscribe from.

        Example:
            client.unsubscribe_topics(["/sample/topic1", "/sample/topic2"])
        """
        self._unsubscribe_topics(topics)

    def listen_forever(self):
        """
        Starts a blocking loop to continuously listen and process incoming events.

        This method initiates an infinite loop that waits for incoming events from the event queue. When an event
        arrives, it is passed to the registered receiver function for further processing. If no receiver function is
        registered at the time of receiving an event, a warning message will be logged and the event will be discarded.

        The method keeps running indefinitely until interrupted by a KeyboardInterrupt (e.g., Ctrl+C). Upon receiving
        such an interrupt, it logs an information message indicating that the client is shutting down.

        Note: This method should be executed in its own thread or process, as it runs indefinitely and can block other
              parts of the program from executing if not properly managed.
        """
        try:
            while True:
                if self.__http_server_thread is None:
                    raise RuntimeError("HTTP server thread has not been started.")
                event = self.__receiver_func_queue.get()
                if self.__receiver_func is None:
                    logging.warn(
                        "Received an event, but no event handler was registered. Discarding event…"
                    )
                    continue
                self.__receiver_func(event)
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


class _HTTPRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, client_instance: Client, *args: Any, **kwargs: Any):
        self.__client_instance = client_instance
        super().__init__(*args, **kwargs)

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
            self.__client_instance._put_incoming_event_into_queue(
                event=Event(data=json_data)
            )

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"msg": "Not Found"}')
