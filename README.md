# SparkDriverLess


## RDD

For every wide dependency transformation, create another repartition RDD between its parent and itself. So that all transformations become narrow dependent, except repartition.


## Client

Background server:

1. Worker discoverer
2. Partition discoverer
3. Job server:
    - After a job is taken by a worker, deactivate the job and set a timer.
    - If timeout and no result returned, reactivate the job for others.

Algorithm:

1. Create RDD lineage by user's input.
2. When `collect` results:
    1. Create partitions lineage, passing in function closure. There is no pointer in partition object to RDD.
    2. For every partition in target RDD, find it in `partition_discoverer`. Fetch it if it exists.
    3. For every missing partition, create a job in `job_server`.
    4. keep discover those missing partitions. Fetch finished partitions from workers and remove that job from `job_server`.
    5. After all partitions are done, merge all results and return.
3. Repeat


## Worker

Background servers:

1. Worker server: broadcast itself as a worker service.
2. Job discoverer
3. Job server
4. Partition discoverer
5. Partition server

Algorithm:

1. Keep trying to get a job from `job_discoverer`.
2. Fetch the partition associated to the job from remote.
3. Check if it's already in `partition_server`. Skip to next job if so.
4. Check all partitions lineage in `partition_discoverer`, get the results of the exists.
5. Run the function of the partition:
    - If it's a narrow dependent partition, or all dependencies are done:
        - Do it right away
    - Otherwise, for every missing dependency:
        1. Broadcast a `job` in `job_server`.
        2. Append current job back to jobs queue.
        3. Skip to next job.
6. When finish the partition successfully, add the result to `partition_server`
7. Repeat
