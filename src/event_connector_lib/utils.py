import enum
from typing import Any, Dict


class Event:
    """
    The Event class encapsulates event-related data and provides various methods
    to access specific elements of the event.

    Attributes:
        data (Dict[Any, Any]): A dictionary containing event data.

    Methods:
        get_event():
            Retrieves the 'event' field from the event data.
        get_payload():
            Retrieves the 'payload' field from the event data.
        get_topic():
            Retrieves the 'topic' field from the 'event' data.
        get_reponse_topic():
            Retrieves the 'respond_to' field from the 'event' data.
        is_response_requested():
            Checks if a response is requested for the event.
        is_response_event():
            Checks if the event is a response event.
    """

    def __init__(self, data: Dict[Any, Any]) -> None:
        self.data = data

    def get_event(self):
        """
        Retrieves the 'event' field from the event data, it the raw metadata
        like the event topic.

        Returns:
            Any: The value of the 'event' field from the event data dictionary, or
            None if the 'event' key is not present.
        """
        return self.data.get("event", None)

    def get_payload(self):
        """
        Retrieves the 'topic' field from the 'event' data.

        Returns:
            Any: The value of the 'topic' field from the 'event' dictionary, or
            None if the 'event' key or 'topic' key is not present.
        """
        return self.data.get("payload", None)

    def get_topic(self):
        """
        Retrieves the 'topic' field from the 'event' data.

        Returns:
            Any: The value of the 'topic' field from the 'event' dictionary, or
            None if the 'event' key or 'topic' key is not present.
        """
        return self.data.get("event", None).get("topic", None)

    def get_reponse_topic(self):
        """
        Retrieves the 'respond_to' field from the 'event' data. The reponse topic is
        meant for follow up events, which the receiver sends to the original sender.

        Returns:
            Any: The value of the 'respond_to' field from the 'event' dictionary, or
            None if the 'event' key or 'respond_to' key is not present.
        """
        return self.data.get("event", None).get("respond_to", None)

    def is_response_requested(self):
        """
        Checks if a response is requested for the event.

        This method looks for the 'response_requested' field within the 'event'
        dictionary in the event data. If this field is set to True, it indicates
        that a response is requested.
        Returns:
            bool: True if a response is requested, False otherwise.
        """
        if self.data.get("event", None).get("response_requested", None) is True:
            return True
        return False

    def is_response_event(self):
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
        if self.get_reponse_topic() is None:
            return False

        return True

    def __str__(self):
        event = self.get_event()
        payload = self.get_payload()
        topic = self.get_topic()
        response_topic = self.get_reponse_topic()
        response_requested = self.is_response_requested()

        return (
            f"Event({{\n"
            f"  'event': {event},\n"
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
