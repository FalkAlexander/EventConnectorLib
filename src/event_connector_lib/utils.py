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
        return self.data.get("event", None)

    def get_payload(self):
        return self.data.get("payload", None)

    def get_topic(self):
        return self.data.get("event", None).get("topic", None)

    def get_reponse_topic(self):
        return self.data.get("event", None).get("respond_to", None)

    def is_response_requested(self):
        if self.data.get("event", None).get("response_requested", None) is True:
            return True
        return False

    def is_response_event(self):
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
