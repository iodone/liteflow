from queue import Queue
from typing import Any, Dict, Optional

from .state import State


class Context:
    stream: Optional[Queue] = None

    def __init__(self):
        self.states = {}  # str -> State

    def get(self, key: str, default: Any = None) -> Any:
        if key not in self.states:
            if default is not None:
                return default
            raise Exception(f"Key {key} not found in context")

        state = self.states[key]
        output = state.get_value()

        return output

    def set(self, key: str, message: Any):
        if key not in self.states:
            state = State()
            state.set_value(message)
            self.states[key] = state
        else:
            state = self.states[key]
            state.set_value(message)

    def set_state(self, key: str, state: State):
        self.states[key] = state

    def set_stream(self, stream: Queue):
        self.stream = stream

    def get_stream(self) -> Queue:
        if self.stream is None:
            raise Exception("Stream is not set. Did you run .stream()?")
        return self.stream

    def to_dict(self) -> Dict[str, Any]:
        return {key: state.get_value_with_lock() for key, state in self.states.items()}

    def from_dict(self, dict: Dict[str, Any]):
        for key, value in dict.items():
            self.set(key, value)
