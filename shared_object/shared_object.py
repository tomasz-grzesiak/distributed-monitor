from threading import Condition
from enum import Enum


class SharedObjectState(Enum):
    UNLOCKED = 1
    ACQUIRING_LOCK = 2
    LOCKED = 3
    WAIT = 4


class VectorClockComparisonResult(Enum):
    GREATER = 1
    SMALLER = 2
    NON_COMPARABLE = 3


class SharedObject:
    def __init__(self, name, connection_manager):
        self.name = name
        self.state = SharedObjectState.UNLOCKED
        self.condition_lock = Condition()
        self.remaining_lock_ack_counter = 0
        self.waiting_for_lock_ack = []
        self.last_state_change_clock = []
        self.notify_id_session = 0
        self.notify_id_session_stored = []
        self.remaining_notify_ack_counter = 0
        self.connection_manager = connection_manager

    def __str__(self):
        return f'SharedObject(name="{self.name}", connection_manager={self.connection_manager})'

    def __enter__(self):
        self.lock()

    def __exit__(self, *args):
        self.unlock()

    def lock(self):
        with self.condition_lock:
            self._verify_state([SharedObjectState.UNLOCKED, SharedObjectState.WAIT])
            self.state = SharedObjectState.ACQUIRING_LOCK
            self.connection_manager.perform_lock(self.name)
            while self.remaining_lock_ack_counter > 0:
                self.condition_lock.wait_for(lambda: self.remaining_lock_ack_counter == 0)
            self.state = SharedObjectState.LOCKED

    def unlock(self):
        with self.condition_lock:
            self._verify_state([SharedObjectState.LOCKED])
            self.state = SharedObjectState.UNLOCKED
            self.connection_manager.perform_unlock(self.name)
            self.remaining_lock_ack_counter = 0
            self.waiting_for_lock_ack = []

    def wait(self):
        with self.condition_lock:
            self._verify_state([SharedObjectState.LOCKED])
            self.state = SharedObjectState.WAIT
            self.connection_manager.perform_unlock(self.name)
            self.connection_manager.perform_wait(self.name)
            self.condition_lock.wait()
            self.notify_id_session = 0
            self.notify_id_session_stored = []
            self.remaining_lock_ack_counter = 0
            self.waiting_for_lock_ack = []
            self.lock()

    def notify(self):
        with self.condition_lock:
            self._verify_state([SharedObjectState.LOCKED])
            self.connection_manager.perform_notify(self.name)

    def notify_all(self):
        with self.condition_lock:
            self._verify_state([SharedObjectState.LOCKED])
            self.connection_manager.perform_notify_all(self.name)

    def compare_clock(self, clock: list) -> VectorClockComparisonResult:
        if len(self.last_state_change_clock) < len(clock):
            self.last_state_change_clock.extend([0] * (len(clock) - len(self.last_state_change_clock)))

        result = VectorClockComparisonResult.NON_COMPARABLE
        for i in range(len(self.last_state_change_clock)):
            if self.last_state_change_clock[i] > clock[i]:
                if result == VectorClockComparisonResult.SMALLER:
                    return VectorClockComparisonResult.NON_COMPARABLE
                result = VectorClockComparisonResult.GREATER
            elif self.last_state_change_clock[i] < clock[i]:
                if result == VectorClockComparisonResult.GREATER:
                    return VectorClockComparisonResult.NON_COMPARABLE
                result = VectorClockComparisonResult.SMALLER
        return result

    # def _is_owned(self) -> bool:
    #     with self.condition_lock:
    #         return self.state == SharedObjectState.LOCKED

    def _verify_state(self, states: list):
        if self.state not in states:
            raise RuntimeError('Shared object in unexpected state! state=%s, expectedStates=%s' % (self.state, states))
