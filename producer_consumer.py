#!/usr/bin/python3
from shared_object import SharedObject
from connection_manager import ConnectionManager
import time


def main():
    connection_manager = ConnectionManager()
    prod_object = SharedObject('producer_object', connection_manager)
    cons_object = SharedObject('consumer_object', connection_manager)
    print(prod_object)
    print(cons_object)
    time.sleep(3)
    print('node_sockets from main thread')
    print(connection_manager.node_sockets)
    prod_object.lock()
    print('lock aquired!')


if __name__ == '__main__':
    main()
