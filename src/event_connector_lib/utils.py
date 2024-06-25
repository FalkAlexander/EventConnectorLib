import enum
from typing import Any, Dict


class InvalidEventDataError(Exception):
    """Raised when the data for creating a module instance is incomplete or invalid."""


class Event:
    """
    The Event class encapsulates event-related data and provides various methods
    to access specific elements of the event. It serves as the data structure for events in the zkms system.

    This class provides a consistent interface for working with events, allowing
    easy access to their components such as header, payload and topics.

    Attributes:
        data (Dict[Any, Any]): A dictionary containing event data.

    Methods:
        header():
            Retrieves the 'event' field from the event data, which contains raw metadata like the event topic.
        payload():
            Retrieves the 'payload' field from the event data.
        topic():
            Retrieves the 'topic' field from the 'event' data.
        reponse_topic():
            Retrieves the 'respond_to' field from the 'event' data.
        is_response_requested():
            Checks if a response is requested for the event.
        is_response_event():
            Checks if the event is a response event.
        __str__():
            Returns a string representation of the Event object, useful for debugging and logging purposes.

    Example:
        Here's an example of how to create an instance of the Event class and access its properties:

        >>> data = {
                "event": {"topic": "example_topic", "respond_to": "response_topic"},
                "payload": {"key1": "value1"}
            }
        >>> event = Event(data)
        >>> print(event.header())
        {'topic': 'example_topic', 'respond_to': 'response_topic'}
    """

    def __init__(self, data: Dict[Any, Any]) -> None:
        """
        Initializes a new instance of the Event class.

        This constructor creates an Event object and sets its internal data attribute based on the provided dictionary. It performs basic validation to ensure that the required 'event' and 'payload' fields are present in the input data. If these fields are missing, it raises a ValueError with an appropriate error message.

        Parameters:
            data (Dict[Any, Any]): A dictionary containing event data, which must include at least an 'event' field and a 'payload' field.

        Raises:
            ValueError: If the input data is missing the required 'event' or 'payload' fields.

        Example Usage:
            >>> data = {
                    "event": {"topic": "example_topic", "respond_to": "response_topic"},
                    "payload": {"key1": "value1"}
                }
            >>> event = Event(data)
        """
        if "event" not in data:
            raise ValueError("Missing required 'event' field in event data")
        if "payload" not in data:
            raise ValueError("Missing required 'payload' field in event data")
        if "topic" not in data["event"]:
            raise ValueError("Missing required 'topic' field in event header")
        self.__data = data

    def get_raw_data(self) -> Dict[Any, Any]:
        """
        Retrieves the raw event data of the event instance.

        This method returns a dictionary representing the raw data stored in
        the module's internal data attribute. The returned dictionary is an unmodified
        version of the original event data that was used to create or update this module.
        It contains all the information related to the event, such as headers and payloads,
        without any processing or modification applied by the Event class itself.
        Returns:
            Dict[Any, Any]: A dictionary containing raw event data associated with the event instance.
        """
        return self.__data

    @property
    def header(self) -> Dict[Any, Any]:
        """
        Retrieves the header from the event data, which contains raw metadata like the event topic and response request status.

        Returns:
            Dict[Any, Any]: A copy of the header of the event.
        """
        return self.__data["event"]

    @property
    def payload(self) -> Dict[Any, Any]:
        """
        Retrieves the 'payload' field from the Event data. The payload contains a custom key-value store of event data.

        Returns:
            Optional[Dict[Any, Any]]: A dictionary containing the event payload.

        Example:
            >>> data = {"event": {"topic": "example_topic", "respond_to": "response_topic"}, "payload": {"key1": "value1"}}
            >>> event = Event(data)
            >>> print(event.payload)
            {'key1': 'value1'}
        """
        return self.__data["payload"]

    @property
    def topic(self) -> str:
        """
        Retrieves the 'topic' field from the header of an Event object.

        This method acts as a getter for accessing the 'topic' attribute within
        the header of an event.

        Returns:
            str: The value of the 'topic' field from the event header.
        """
        return self.header["topic"]

    @property
    def access_token(self) -> str | None:
        """
        Retrieves the 'token' field from the header of an Event object.

        This method acts as a getter for accessing the 'token' attribute within
        the header of an event, which represents an access token. If the 'token'
        field is not present in the header, this method returns None.

        Returns:
            str | None: The value of the 'token' field from the event header if it exists, or None otherwise.
        """
        return self.header.get("token", None)

    @property
    def response_requested(self) -> bool:
        """
        Checks if a response is requested for the event.

        This method looks for the 'response_requested' field within the 'event'
        dictionary in the event data. If this field is set to True, it indicates
        that a response is requested.
        Returns:
            bool: True if a response is requested, False otherwise.
        """
        return self.header["response_requested"]

    @property
    def response_topic(self) -> str | None:
        """
        Retrieves the 'respond_to' field from the 'event' data. The reponse topic is
        meant for follow up events, which the receiver sends to the original sender.

        Returns:
            Any: The value of the 'respond_to' field from the 'event' dictionary, or
            None if the 'event' key or 'respond_to' key is not present.
        """
        return self.header.get("respond_to", None)

    def has_access_token(self) -> bool:
        """
        Checks if the event header contains an access token.

        This method checks whether the 'header' attribute of this Event instance contains a key named "token". If such a key is present, it means that the event includes an access token, and the method returns True; otherwise, it returns False.
        Returns:
            bool: True if the event header contains an access token, False otherwise.
        """
        if "token" in self.header:
            return True
        return False

    def is_response_event(self) -> bool:
        """
        Checks if the event is a response event.

        This method determines if the event contains a 'respond_to' field
        within the 'event' dictionary, which indicates that the event is
        a response to a previous event.
        Returns:
            bool: True if the 'respond_to' key is present within the 'event'
            dictionary, indicating that the event is a response event;
            False otherwise.
        """
        if self.response_topic is None:
            return False

        return True

    def get_payload_object(self, json_path: str):
        """
        Retrieve an object from the payload using a JSON path.

        Args:
            json_path (str): A string representing the JSON path to the desired object.

        Returns:
            Any: The object retrieved from the JSON path, or None if it doesn't exist.

        Raises:
            KeyError: If any of the keys in the JSON path do not exist.
        """
        keys = json_path.split(".")
        value = self.payload

        for key in keys:
            if key in value:
                value = value[key]
            else:
                raise KeyError(f"Key '{json_path}' not found in payload.")

        return value

    def __str__(self) -> str:
        header = self.header
        payload = self.payload
        topic = self.topic
        response_requested = self.response_requested
        response_topic = self.response_topic

        return (
            f"Event({{\n"
            f"  'header': {header},\n"
            f"  'payload': {payload},\n"
            f"  'topic': {topic},\n"
            f"  'respond_to': {response_topic},\n"
            f"  'response_requested': {response_requested}\n"
            f"}})"
        )


class BrokerEvent(Event):
    """
    The BrokerEvent class represents a specialized type of Event that originates from
    a broker and has a static destination. This allows the broker to send events to
    specific locations, such as modules, for its internal implementation use.

    Attributes:
        destination (str): The static destination of the event, indicating where
            it should be sent or handled within the system. This is most likely a HTTP endpoint.

    Methods:
        get_destination():
            Retrieves the destination attribute of the BrokerEvent instance.
    """

    def __init__(self, destination: str, **kwargs: Any):
        self.__destination = destination
        super(BrokerEvent, self).__init__(**kwargs)

    @property
    def destination(self) -> str:
        return self.__destination

    @staticmethod
    def from_event(event: Event, destination: str) -> "BrokerEvent":
        """
        Create a new BrokerEvent object based on an existing Event instance with only the additional destination parameter.

        :param data: The source Event raw data.
        :type data: Dict[Any, Any]
        :param destination: The destination for the BrokerEvent.
        :type destination: str
        :return: A new BrokerEvent object initialized with data from the source Event and the provided destination.
        :rtype: BrokerEvent
        """
        return BrokerEvent(data=event.get_raw_data(), destination=destination)


class ModuleType(enum.Enum):
    """
    Enumeration for different types of ZKMS Modules.

    Attributes:
    CORE (int): Represents a core module within the ZKMS System.
    SUPPORT (int): Represents a support module within the ZKMS System.
    AI (int): Represents an AI module within the ZKMS System.
    BROKER (int): Represents an module that implementes broker functionality within the ZKMS System.
    """

    CORE = 0
    SUPPORT = 1
    AI = 2
    BROKER = 3


class Module:
    """
    Represents an abstract concept of a module within the ZKMS System. This class is used to describe and manage
    modules in a structured manner, specifying their name, description, version, type, event handler, and topics they are subscribed to.
    Please note that this class does not actually implement a module; it's a blueprint for one.

    Examples:
        Creating a support module that handles events related to 'user_support' and sends them to 'http://127.0.0.1:56789/events':

        >>> support_module = Module(name="Support", description="Handles support events", version="1.0.0", type=ModuleType.SUPPORT, event_handler="http://127.0.0.1:56789/events", topics=["/support"])
        >>> print(support_module.name)
        Support

    Attributes:
        name (str): The name of the module. This attribute should not be changed once set.
        description (str): A brief description of the module's purpose and functionality. This attribute should not be changed once set.
        version (str): The current version of the module, following semantic versioning standards. This attribute should not be changed once set.
        type (ModuleType): The type of the module within the ZKMS System. This attribute should not be changed once set.
        event_handler (str): The URL to which events will be sent by this module. This is typically an HTTP endpoint. This attribute should not be changed once set.
        topics (list[str]): A list of topics that the module is subscribed to, and for which it will handle events. Topics can be added or removed dynamically using the provided methods.
    """

    def __init__(
        self,
        name: str,
        description: str,
        version: str,
        type: ModuleType,
        event_handler: str,
        topics: list[str],
    ):
        """
        Initializes a new instance of the module.

        :param name: The name of the module.
        :type name: str
        :param description: A brief description of the module's purpose and functionality.
        :type description: str
        :param version: The current version of the module, following semantic versioning standards.
        :type version: str
        :param type: The type of the module within the ZKMS System.
        :type type: ModuleType
        :param event_handler: The URL to which events will be sent by this module (typically an HTTP endpoint).
        :type event_handler: str
        :param topics: A list of topics that the module is subscribed to and for which it will handle events.
        :type topics: list[str]

        These attributes (except of topics) are properties and not intended for being changed directly after initialization.

        Example code snippet:

        >>> support_module = Module(
                name='Support',
                description='A module that provides support functionality.',
                version='1.0.0',
                type=ModuleType.SUPPORT,
                event_handler='hhttp://127.0.0.1:56789/events',
                topics=['/support']
            )
        >>> print(support_module.name)
        Support
        """
        self.__name = name
        self.__description = description
        self.__version = version
        self.__type = type
        self.__event_handler = event_handler
        self.__topics = set(topics)

    @property
    def name(self) -> str:
        """
        Returns the name of the module. This property should not be changed after initialization.

        :return: The name of the module as a string.
        """
        return self.__name

    @property
    def description(self) -> str:
        """
        Returns the description of the module, which provides an overview or explanation of its functionality.
        This property should not be changed after initialization.

        :return: The description of the module as a string.
        """
        return self.__description

    @property
    def version(self) -> str:
        """
        Returns the current version of the module.
        This property should not be changed after initialization.

        :return: The version number of the module as a string.
        """
        return self.__version

    @property
    def type(self) -> ModuleType:
        """
        Returns the type of the module within the ZKMS System.
        This property should not be changed after initialization.

        :return: The module type as an instance of the ModuleType enumeration class.
        """
        return self.__type

    @property
    def event_handler(self) -> str:
        """
        Returns the URL to which events will be sent intended for this module.
        This property should not be changed after initialization.

        :return: The event handler's URL as a string.
        """
        return self.__event_handler

    @property
    def topics(self) -> list[str]:
        """
        Returns a copy of the current list of topics that the module is subscribed to and handles events for.
        The topics are returned as a list of strings. Note that modifying this list will not update the module's subscription; use add_topic() or remove_topic() methods for that.

        :return: A copy of the current topic list as a list of strings.
        """
        return list(self.__topics)

    @property
    def access_token(self) -> str | None:
        """
        Returns the current access token that the module uses to authenticate with external services. This is a read-only property. If no access token has been set, this will return None.

        :return: The current access token as a string, or None if no access token has been set.
        """
        return self.__token

    def add_topic(self, topic: str):
        """
        Adds a new topic to the module's subscription list. If the topic already exists in the subscription list, this method does nothing.

        :param topic: The topic to be added as a string.
        :type topic: str
        """
        self.__topics.add(topic)

    def add_topics(self, topics: list[str]):
        """
        Adds multiple new topics to the module's subscription list. If a topic already exists in the subscription list, it will not be added again.

        :param topics: A list of topics to be added as strings.
        :type topics: list[str]
        """
        self.__topics.update(topics)

    def remove_topic(self, topic: str):
        """
        Removes a specific topic from the module's subscription list. If the topic does not exist in the subscription list, this method will raise a KeyError.

        :param topic: The topic to be removed as a string.
        :type topic: str
        :raises KeyError: When attempting to remove a non-existent topic.
        """
        self.__topics.remove(topic)

    def remove_topics(self, topics: list[str]):
        """
        Removes multiple specific topics from the module's subscription list. If a topic does not exist in the subscription list, it will be ignored and no error will be raised.

        :param topics: A list of topics to be removed as strings.
        :type topics: list[str]
        """
        self.__topics.difference_update(topics)

    def set_access_token(self, token: str):
        """
        Sets the access token for the module instance. The access token is used to authenticate and authorize requests made by this module.

        :param token: The access token as a string.
        :type token: str
        """
        self.__token = token

    def as_dict(self):
        return {
            "name": self.__name,
            "description": self.__description,
            "version": self.__version,
            "type": self.__type.value,
            "event_handler": self.__event_handler,
            "topics": list(self.__topics),
        }

    def __str__(self):
        return (
            f"Module(\n"
            f"\tname={self.__name},\n"
            f"\tdescription={self.__description},\n"
            f"\tversion={self.__version},\n"
            f"\ttype={self.__type.value},\n"
            f"\tevent_handler={self.__event_handler},\n"
            f"\ttopics={list(self.__topics)}\n)"
        )
