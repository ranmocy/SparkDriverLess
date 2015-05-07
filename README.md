# MicroSpark


## Client
1. discover `worker`, append to workers
2. discover `rdd`, append to rdds
3. start partitions_server
    - TODO: if it's taken, set a timer.
    - If timeout and no result, broadcast again since that worker is too slow.
4. start console for the user
    - when transition:
        1. create rdd lineage
    - when action:
        1. create partitions from rdds (partition_num = len(workers))
        2. try search target_rdd in rdds
            - if exists, fetch result from corresponding worker
            - if doesn't exist, or previous try failed
                - for every partition of target_rdd:
                    1. append to partitions_server
                    2. broadcast a `job` with partition uuid
        3. keep discovering rdds until found the target_rdd
        4. stop broadcast the `job`
        5. retrieve result of the rdd
5. repeat


## Worker
1. broadcast a `worker` with new generated uuid, {address=ip:port}
2. discover `job`, append to jobs
3. discover `rdd`, append to rdds
4. start a result server
5. start a partitions_server
    - TODO: if it's taken, set a timer.
    - If timeout and no result, broadcast again since that worker is too slow.
6. start a loop keep trying to get a job from jobs:
    1. connect to job's source, lock it up to prevent other workers to take it
    2. get the dumped_rdd, unload it
    3. run the target_rdd
        - if narrow_dependent:
            - do it right away
        - if wide_dependent:
            - try search dep_rdd in rdds
                - if exists, fetch result from corresponding worker
                - if doesn't exist, or previous try failed
                    1. for every partition of dep_rdd:
                        1. append to partitions_server
                        2. broadcast a `job` with partition uuid
                    2. append current job back to jobs
                    3. DO NOT sleep(NETWORK_LATENCY * 2). it's better to it locally to avoid network transfer
                    3. continue to next job
    4. add result to the result server
    5. broadcast this rdd


## TODO
1. prevent multiple worker to get the same job: lock?
2. worker partition_server = zerorpc.server(), `fetch_partition(uuid)`. Returns partition result in array
