from threading import Lock, Semaphore
from typing import Any, Optional


class State:
    def __init__(self, value: Optional[Any] = None):
        self._value = value
        self.semaphore = Semaphore(0)
        self._lock = Lock()

    # acquiring semaphore first, then returning the message
    def get_value(self) -> Optional[Any]:
        self.semaphore.acquire()
        output = self._value
        self.semaphore.release()
        return output

    def get_value_with_lock(self) -> Optional[Any]:
        with self._lock:
            return self._value

    def set_value(self, value: Optional[Any]):
        with self._lock:
            self._value = value
        # adding single permit after value is set
        self.semaphore.release()

    @classmethod
    def empty(cls) -> "State":
        return cls(None)
