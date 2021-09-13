from threading import Condition
from enum import Enum


class SharedObjectState(Enum):
    INACTIVE = 1
    WAITING_FOR_LOCK_ACK = 2
    LOCKED = 3


class VectorClockComparisonResult(Enum):
    GREATER = 1
    SMALLER = 2
    NON_COMPARABLE = 3


class SharedObject:
    def __init__(self, name, connection_manager):
        self.name = name
        self.state = SharedObjectState.INACTIVE
        self.condition_lock = Condition()
        self.remaining_lock_ack_counter = 0
        self.waiting_for_lock_ack = []
        self.last_state_change_clock = []
        self.connection_manager = connection_manager

    def __str__(self):
        return f'SharedObject(name="{self.name}", connection_manager={self.connection_manager})'

    def lock(self):
        self.condition_lock.acquire()
        self.state = SharedObjectState.WAITING_FOR_LOCK_ACK
        self.connection_manager.perform_lock(self.name)
        self.condition_lock.wait_for(lambda: self.remaining_lock_ack_counter == 0)
        print('ZDOBYTO LOCK!!!')
        self.state = SharedObjectState.LOCKED
        self.condition_lock.release()

    def unlock(self):
        self.condition_lock.acquire()
        self.state = SharedObjectState.INACTIVE
        self.connection_manager.perform_unlock(self.name)
        self.waiting_for_lock_ack = []
        self.condition_lock.release()

    def wait(self):
        pass

    def notify(self):
        pass

    def notify_all(self):
        pass

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
