# Flink Command-Line Mode Quickstart

In this document, we will show you the steps to submit a simple FeatHub job with Flink 
Commandline Tool to a standalone Flink cluster. The FeatHub job simply consumes
the data from the Flink datagen connector, computes some feature, and prints out the 
result.

## Prerequisites

- Unix-like operating system (e.g. Linux, Mac OS X)
- Python 3.7
- Java 8

## Install Flink

Download a stable release of Flink 1.16.1, then extract the archive:

```bash
$ curl -LO https://archive.apache.org/dist/flink/flink-1.16.1/flink-1.16.1-bin-scala_2.12.tgz
$ tar -xzf flink-1.16.1-bin-scala_2.12.tgz
```

You can refer to the [local installation](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/try-flink/local_installation/) 
instruction for more detailed step.

## Deploy a Standalone Flink cluster

You can deploy a standalone Flink cluster in your local environment with the following 
command.

```bash
$ ./flink-1.16.1/bin/start-cluster.sh
```

You should be able to navigate to the web UI at [localhost:8081](http://localhost:8081)
to view the Flink dashboard and see that the cluster is up and running.

## Package FeatHub Python Dependencies

Different Flink deployment modes have different ways to manage the dependencies for the
Flink job. For example, when submitting to Yarn Cluster, user can specify a local zip 
file that contains the dependencies. When submitting to Kubernetes cluster, user has
to install the dependencies to the Docker image.

We provide the script to package a zip that contains the Python dependencies to run
FeatHub job. You can build the Python dependencies with nightly version of FeatHub or 
with a FeatHub wheel with the commands below.

```bash
# Build FeatHub Python Dependencies with nightly version of FeatHub
$ bash tools/cli-deps/build-cli-deps.sh
```

The dependencies zip will be available at `tools/cli-dep/deps.zip`. We will submit the 
Flink job together with the zip file to the standalone Flink cluster.

You can modify `tools/cli-dep/requirements.txt` to include any additional Python 
dependencies to the `deps.zip`.

If the Python dependencies is a [Platform Wheel](https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#platform-wheels)
that are specific to certain platform, like Linux, macOS, or Windows. You need to make
sure that the environment that run the building script is the same as the environment
the Flink cluster runs.

## Deploy FeatHub Job in the Flink Cluster

Now you can submit the FeatHub job to the standalone Flink cluster with the following
command:

```bash
$ ./flink-1.16.1/bin/flink run --python python/feathub/examples/streaming_average_flink_cli.py \
    --pyFiles tools/cli-deps/deps.zip
```

## Result

When the job is running, you can check the output at the TaskManager stdout.
