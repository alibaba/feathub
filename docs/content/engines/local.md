# Local Processor 

[Local
Processor](https://github.com/alibaba/feathub/blob/master/python/feathub/processors/local/local_processor.py)
utilizes CPUs on the local machine to compute features and uses Pandas
DataFrame to store tabular data in memory. It is useful for doing experiments
on a local machine without having to deploy and connect to a distributed
Flink/Spark cluster.

This processor is implemented using the Pandas library and computes features in
the given Python process. If the feathub-nightly[spark] is installed, the Local
processor can utilize Spark's local mode for accessing storages (e.g. HDFS) that
it otherwise would not support.

See [here](../../../README.md#quickstart) for an example of using Local Processor.

