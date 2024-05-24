from typing import Any, Dict


class Event:
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
