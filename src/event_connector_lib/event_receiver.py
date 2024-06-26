from typing import Callable, Dict
from client import Client
from utils import Event


class EventReceiver:
    def __init__(self, client: Client) -> None:
        self.__client = client
        self.__event_handlers: Dict[str, Callable[[Event], None]] = {}

    def topic_handler(
        self, topic: str
    ) -> Callable[[Callable[[Event], None]], Callable[[Event], None]]:
        """Register an event handler function for the specified topic."""

        def decorator(f: Callable[[Event], None]):
            self.__event_handlers[topic] = f
            self.__client.set_event_handler(receiver_func=self.__handle_events)
            return f

        return decorator

    def __handle_events(self, event: Event) -> None:
        handler = self.__event_handlers.get(event.topic)
        if handler is None:
            raise ValueError(f"No handler registered for topic '{event.topic}'")
        return handler(event)
