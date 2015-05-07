#!/usr/bin/env python
import logging

from rdd import Context
from cli import CLI


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, filename='client.log', filemode='a')
    logging.info("\n=====Client Start=====\n")
    CLI(local={'context': Context()})
