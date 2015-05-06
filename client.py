#!/usr/bin/env python
import logging

from worker_discover import WorkerDiscover
from partition_discover import PartitionDiscover
from partition_server import PartitionServer
from rdd import Context
from cli import CLI


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename='client.log', filemode='a')

    # 1. discover `worker`, append to workers
    # 2. discover `partitions`, append to partitions
    # 3. start partitions_server
    # 4. start console for the user
    CLI(local={'context': Context(worker_discover=WorkerDiscover(),
                                  partition_discover=PartitionDiscover(),
                                  partition_server=PartitionServer())})
