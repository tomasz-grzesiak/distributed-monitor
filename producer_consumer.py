from shared_object import SharedObject
from connection_manager import ConnectionManager
import time


def main():
    connection_manager = ConnectionManager()
    prod_object = SharedObject('producer_object', connection_manager)
    cons_object = SharedObject('consumer_object', connection_manager)
    print(prod_object)
    print(cons_object)
    time.sleep(5)


if __name__ == '__main__':
    main()
