class SharedObject:
    def __init__(self, name, connection_manager):
        self.name = name
        self.connection_manager = connection_manager

    def __str__(self):
        return f'SharedObject(name="{self.name}", connection_manager={self.connection_manager})'

    def lock(self):
        self.connection_manager.perform_lock(self.name)

    def unlock(self):
        pass

    def wait(self):
        pass

    def notify(self):
        pass

    def notify_all(self):
        pass
