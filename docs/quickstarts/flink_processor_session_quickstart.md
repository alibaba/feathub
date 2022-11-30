## Flink Processor Quickstart

In this document, we demonstrate how to run Feathub with Flink processor and  compute 
the Feathub feature with a local standalone Flink cluster.

### Install Flink 

Download a stable release of Flink 1.15.2, then extract the archive:

```bash
$ curl -LO https://archive.apache.org/dist/flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz
$ tar -xzf flink-1.15.2-bin-scala_2.12.tgz
```

You can refer to the [local installation](https://nightlies.apache.org/flink/flink-docs-release-1.15//docs/try-flink/local_installation/) 
instruction for more detailed step.

### Start the standalone Flink cluster locally

You can start a Flink standalone cluster in your local environment with the following 
command.

```bash
$ ./flink-1.15.2/bin/start-cluster.sh
```

You should be able to navigate to the web UI at [localhost:8081](http://localhost:8081)
to view the Flink dashboard and see that the cluster is up and running.

### Run demo
Execute the following command to run the
[nyc_taxi_flink_session.py](../../python/feathub/examples/nyc_taxi_flink_session.py) 
demo.

```bash
$ python python/feathub/examples/nyc_taxi_flink_session.py
```
