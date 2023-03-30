# Spark Quickstart

In this document, we will show you the steps to submit a simple FeatHub job
to a standalone Spark cluster. The FeatHub job simply consumes the data from
the local filesystem, computes some feature, and prints out the result.

## Prerequisites

- Unix-like operating system (e.g. Linux, Mac OS X)
- Python 3.7
- Java 8

## Install Spark

Download a stable release of Spark 3.3.1, then extract the archive:

```bash
$ curl -LO https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
$ tar -xzf spark-3.3.1-bin-hadoop3.tgz
```

## Deploy a Standalone Spark cluster

You can deploy a standalone Spark cluster in your local environment with the following 
command.

```bash
$ ./spark-3.3.1-bin-hadoop3/sbin/start-all.sh
```

You should be able to navigate to the web UI at [localhost:8080](http://localhost:8080)
to view the Spark dashboard and see that the cluster is up and running.

## Install FeatHub Python Library with Spark Support

```bash
$ python -m pip install --upgrade "feathub-nightly[spark]"
```

## Run demo
Execute the following command to run the
[nyc_taxi_spark_client.py](../../../../python/feathub/examples/nyc_taxi_spark_client.py) 
demo.

```bash
$ python python/feathub/examples/nyc_taxi_spark_client.py
```
