#!/usr/bin/env python
import logging

from rdd import Context
from cli import CLI


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename='client.log', filemode='a')
    CLI(local={'context': Context()})