#!/usr/bin/python3
from shared_object_context import SharedObjectContext
import time


def main():
    context = SharedObjectContext()
    prod_object = context.shared_object('producer_object')
    cons_object = context.shared_object('consumer_object')
    print(prod_object)
    print(cons_object)
    time.sleep(3)
    prod_object.lock()
    print('lock aquired!')


if __name__ == '__main__':
    main()
