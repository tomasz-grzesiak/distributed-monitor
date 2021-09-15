#!/usr/bin/python3
from shared_object import SharedObjectContext
import time
from random import randint


def main():
    context = SharedObjectContext()
    shared_object = context.shared_object('object')
    while True:
        time.sleep(randint(2, 4))
        shared_object.lock()
        print('Lock acquired')
        time.sleep(randint(3, 5))
        shared_object.unlock()
        print('Lock released')


if __name__ == '__main__':
    main()
