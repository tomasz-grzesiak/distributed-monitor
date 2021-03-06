#!/usr/bin/python3
from shared_object import SharedObjectContext
import time


def main():
    context = SharedObjectContext()
    prod_object = context.shared_object('producer_object')
    cons_object = context.shared_object('consumer_object')
    time.sleep(3)
    print('waiting for lock...')
    prod_object.lock()
    print('lock aquired!')
    time.sleep(5)
    prod_object.unlock()
    print('lock released')


if __name__ == '__main__':
    main()
