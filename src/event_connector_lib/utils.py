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
        self.destination = destination
        super(BrokerEvent, self).__init__(**kwargs)

    def get_destination(self):
        return self.destination
