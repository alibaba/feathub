# Flink Session Mode Quickstart

In this document, we will show you the steps to submit a simple FeatHub job
to a standalone Flink cluster in session mode. The FeatHub job simply consumes
the data from the Flink datagen connector, computes some feature, and prints 
out the result.

## Prerequisites

- Unix-like operating system (e.g. Linux, Mac OS X)
- Python 3.7
- Java 8

## Install Flink

Download a stable release of Flink 1.15.2, then extract the archive:

```bash
$ curl -LO https://archive.apache.org/dist/flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz
$ tar -xzf flink-1.15.2-bin-scala_2.12.tgz
```

You can refer to the [local installation](https://nightlies.apache.org/flink/flink-docs-release-1.15//docs/try-flink/local_installation/) 
instruction for more detailed step.

## Install FeatHub Python Library with Flink Support

```bash
# If you are using Flink processor, run the following command
$ python -m pip install --upgrade "feathub-nightly[flink]"
```

## Deploy a Standalone Flink cluster

You can deploy a standalone Flink cluster in your local environment with the following 
command.

```bash
$ ./flink-1.15.2/bin/start-cluster.sh
```

You should be able to navigate to the web UI at [localhost:8081](http://localhost:8081)
to view the Flink dashboard and see that the cluster is up and running.

## Run demo
Execute the following command to run the
[nyc_taxi_flink_session.py](../../../../python/feathub/examples/nyc_taxi_flink_session.py) 
demo.

```bash
$ python python/feathub/examples/nyc_taxi_flink_session.py
```
