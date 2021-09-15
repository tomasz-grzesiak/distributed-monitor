#!/usr/bin/python3
from shared_object import SharedObjectContext
import time
from random import randint


def main():
    context = SharedObjectContext()
    shared_object = context.shared_object('object')
    while True:
        time.sleep(randint(2, 4))
        with shared_object:
            print('Lock acquired')
            time.sleep(randint(2, 3))
            shared_object.notify()
            print('Sent notify')


if __name__ == '__main__':
    main()
