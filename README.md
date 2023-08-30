FeatHub is a stream-batch unified feature store that simplifies feature
development, deployment, monitoring, and sharing for machine learning
applications.

- [Introduction](#introduction)
- [Core Benefits](#core-benefits)
- [What you can do with FeatHub](#what-you-can-do-with-feathub)
- [Architecture Overview](#architecture-overview)
- [Supported Compute Engines](#supported-compute-engines)
- [FeatHub SDK Highlights](#feathub-sdk-highlights)
- [User Guide](#user-guide)
  * [Prerequisites](#prerequisites)
  * [Install FeatHub Nightly Build](#install-feathub-nightly-build)
  * [Quickstart](#quickstart)
  * [Examples](#examples)
- [Developer Guide](#developer-guide)
- [Roadmap](#roadmap)
- [Contact Us](#contact-us)
- [Additional Resources](#additional-resources)

## Introduction

FeatHub is an open-source feature store designed to simplify the development and
deployment of machine learning models. It supports feature ETL and provides an
easy-to-use Python SDK that abstracts away the complexities of point-in-time
correctness needed to avoid training-serving skew. With FeatHub, data scientists
can speed up the feature deployment process and optimize feature ETL by
automatically compiling declarative feature definitions into performant
distributed ETL jobs using state-of-the-art computation engines of their choice,
such as Flink or Spark.

Checkout [Documentation](docs/content) for guidance on compute
engines, connectors, expression language, and more.


## Core Benefits

Similar to other feature stores, FeatHub provides the following core benefits:

- **Simplified feature development**: The Pythonic [FeatHub
SDK](docs/content/feathub-sdk) makes it easy to develop features without worrying
about point-in-time correctness.  This helps to avoid training-serving skew,
which can negatively impact the accuracy of machine learning models.
- **Faster feature deployment**: FeatHub automatically compiles user-specified
declarative feature definitions into performant distributed ETL jobs using
state-of-the-art computation engines, such as Flink or Spark. This speeds up
the feature deployment process and eliminates the need for data engineers to
re-write Python programs into distributed stream or batch processing jobs.
- **Performant feature generation**: FeatHub offers a range of [built-in
  optimizations](docs/content/deep-dive/optimizations.md) that leverage commonly
observed feature ETL job patterns. These optimizations are automatically applied
to ETL jobs compiled from the declarative feature definitions, much like how SQL
optimizations are applied.
- **Facilitated feature sharing**: FeatHub allows developers to register and
query feature definitions in a persistent [feature
registry](docs/content/registries). This capability reduces the duplication of
data engineering efforts and the resource cost of feature generation by
allowing developers in the organization to share and re-use existing feature
definitions and datasets.

In addition to the above benefits, FeatHub provides several architectural
benefits compared to other feature stores, including:

- **Real-time feature generation**: FeatHub supports real-time feature
generation using [Apache Flink](docs/content/engines/flink.md) as the stream
computation engine with milli-second latency. This provides better performance
than other open-source feature stores that only support feature generation
using Apache Spark.

- **Assisted feature monitoring**: FeatHub provides [built-in
metrics](docs/content/metric-stores) to monitor the quality of features and
alert users to issues such as feature drift. This helps to improve the accuracy
and reliability of machine learning models.

- **Stream-batch unified computation**: FeatHub allows for consistent feature
computation across offline, nearline, and online stacks using [Apache
Flink](docs/content/engines/flink.md) for real-time features with low latency,
[Apache Spark](docs/content/engines/spark.md) for offline features with high
throughput, and FeatureService for computing features online when the request
is received.

- **Extensible framework**: FeatHub's Python SDK is decoupled from the APIs of
the underlying computation engines, providing flexibility and avoiding lock-in.
This allows for the support of additional computation engines in the future.
For example, FeatHub supports [Local
Processor](docs/content/engines/local.md) that is implemented using Pandas
library, in addition to its support for Apache Flink and Apache Spark.

Usability is a crucial factor that sets feature store projects apart. Our SDK is
designed to be **Pythonic**, **declarative**, intuitive, and highly expressive to
support all the necessary feature transformations. We understand that a feature
store's success depends on its usability as it directly affects developers'
productivity. Check out the [FeatHub SDK Highlights](#feathub-sdk-highlights)
section below to learn more about the exceptional usability of our SDK.


<!-- TODO: provide examples showing the advantage of python SDK over SQL. -->

## What you can do with FeatHub

With FeatHub, you can:
- **Define new features**: Define features as the result of applying
expressions, aggregations, and cross-table joins on existing features, all with
point-in-time correctness.
- **Read and write features data**: Read and write feature data into a variety
  of offline, nearline, and online [storage
systems](docs/content/connectors) for both offline training and online
serving.
- **Backfill features data**: Process historical data with the given time range
and/or keys to backfill feature data, whic
- **Run experiments**: Run experiments on the local machine using
LocalProcessor without connecting to Apache Flink or Apache Spark cluster. Then
deploy the FeatHub program in a distributed Apache Flink or Apache Spark
cluster by changing the program configuration.

## Architecture Overview

The architecture of FeatHub and its key components are shown in the figure below.

<img src="docs/static/img/architecture_1.png" width="50%" height="auto">

The workflow of defining, computing, and serving features using FeatHub is illustrated in the figure below.

<img src="docs/static/img/architecture_2.png" width="70%" height="auto">

See [Basic Concepts](docs/content/basic-concepts.md) for more details about the key components in FeatHub.

## Supported Compute Engines

FeatHub supports the following compute engines to execute feature ETL pipeline:
- [Apache Flink 1.16](docs/content/engines/flink.md)
- [Aapche Spark 3.3](docs/content/engines/spark.md)
- [Local Processor](docs/content/engines/local.md)

## FeatHub SDK Highlights

The following examples demonstrate how to define a variety of features
concisely using FeatHub SDK. See [FeatHub
SDK](docs/content/feathub-sdk) for more details.

See [NYC Taxi Demo](docs/examples/nyc_taxi.ipynb) to learn more about how to
define, generate and serve features using FeatHub SDK.

- Define features via table joins with point-in-time correctness

```python
f_price = Feature(
    name="price",
    transform=JoinTransform(
        table_name="price_update_events",
        feature_name="price"
    ),
    keys=["item_id"],
)
```

- Define over-window aggregation features:

```python
f_total_payment_last_two_minutes = Feature(
    name="total_payment_last_two_minutes",
    transform=OverWindowTransform(
        expr="item_count * price",
        agg_func="SUM",
        window_size=timedelta(minutes=2),
        group_by_keys=["user_id"]
    )
)
```

- Define sliding-window aggregation features:

```python
f_total_payment_last_two_minutes = Feature(
    name="total_payment_last_two_minutes",
    transform=SlidingWindowTransform(
        expr="item_count * price",
        agg_func="SUM",
        window_size=timedelta(minutes=2),
        step_size=timedelta(minutes=1),
        group_by_keys=["user_id"]
    )
)
```

- Define features via built-in functions and the FeatHub expression language:

```python
f_trip_time_duration = Feature(
    name="f_trip_time_duration",
    transform="UNIX_TIMESTAMP(taxi_dropoff_datetime) - UNIX_TIMESTAMP(taxi_pickup_datetime)",
)
```

- Define a feature via Python UDF:

```python
f_lower_case_name = Feature(
    name="lower_case_name",
    dtype=types.String,
    transform=PythonUdfTransform(lambda row: row["name"].lower()),
)
```

<!-- TODO: Add SqlFeatureView. -->

## User Guide

Checkout [Documentation](docs/content) for guidance on compute
engines, connectors, expression language, and more.

### Prerequisites

You need the following to run FeatHub installed using pip:
- Unix-like operating system (e.g. Linux, Mac OS X)
- Python 3.7/3.8/3.9

### Install FeatHub Nightly Build


To install the nightly version of FeatHub and the corresponding extra
requirements based on the compute engine you plan to use, run one of the
following commands:

```bash
# Run the following command if you plan to run FeatHub using a local process
$ python -m pip install --upgrade feathub-nightly

# Run the following command if you plan to use Apache Flink cluster
$ python -m pip install --upgrade "feathub-nightly[flink]"

# Run the following command if you plan to use Apache Spark cluster, or to use
# Spark-supported storage in a local process. 
$ python -m pip install --upgrade "feathub-nightly[spark]"
```

### Quickstart

#### Quickstart using Local Processor

Execute the following command to compute features defined in
[nyc_taxi.py](python/feathub/examples/nyc_taxi.py) in the given Python process.

```bash
$ python python/feathub/examples/nyc_taxi.py
```

#### Quickstart using Flink Processor

You can use the following quickstart guides to compute features in a Flink
cluster with different deployment modes:

- [Flink Processor Session Mode Quickstart](docs/content/quickstarts/flink-session-mode.md)
- [Flink Processor Cli Mode Quickstart](docs/content/quickstarts/flink-cli-mode.md)

#### Quickstart using Spark Processor

You can use the following quickstart guides to compute features in a standalone
Spark cluster.

- [Spark Processor Client Mode Quickstart](docs/content/quickstarts/spark-client-mode.md)

### Examples

The following examples can be run on Google Colab.

| Name                                                         | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| [NYC Taxi Demo](./docs/examples/nyc_taxi.ipynb)              | Quickstart notebook that demonstrates how to define, extract, transform and materialize features with NYC taxi-fare prediction sample data. |
| [Feature Embedding Demo](./docs/examples/feature_embedding.ipynb) | FeatHub UDF example showing how to define and use feature embedding with a pre-trained Transformer model and hotel review sample data. |
| [Fraud Detection Demo](./docs/examples/fraud_detection.ipynb) | An example to demonstrate usage with multiple data sources such as user account and transaction data. |

Examples in this [this](https://github.com/flink-extended/feathub-examples)
repo can be run using docker-compose.


## Developer Guide

### Prerequisites

You need the following to build FeatHub from source:
- Unix-like operating system (e.g. Linux, Mac OS X)
- x86_64 architecture
- Python 3.7/3.8/3.9
- Java 8
- Maven >= 3.1.1

### Install Development Dependencies

1. Install the required Python libraries.

```bash
$ python -m pip install -r python/dev-requirements.txt
```
 
2. Start docker engine and pull the required images.

```bash
$ docker image pull redis:latest
$ docker image pull confluentinc/cp-kafka:5.4.3
```

3. Increase open file limit to be at least 1024.

```bash
$ ulimit -n 1024
```

### Build and Install FeatHub from Source
<!-- TODO: Add instruction to install "./python[all]" after the dependency confliction in PyFlink and PySpark is resolved. -->
```bash
$ mvn clean package -DskipTests -f ./java
$ python -m pip install "./python[flink]"
$ python -m pip install "./python[spark]"
```

### Run Tests

Please execute the following commands under Feathub's root folder to run tests.

```bash
$ mvn clean package -f ./java
$ pytest --tb=line -W ignore::DeprecationWarning ./python
```

While the commands above cover most of Feathub's tests, some FlinkProcessor's
python tests, such as tests related to Parquet format, have been ignored by
default as they require a Hadoop environment to function correctly. In order to
run these tests, please install Hadoop on your local machine and set up
environment variables as follows before executing the commands above.

```bash
export FEATHUB_TEST_HADOOP_CLASSPATH=`hadoop classpath`
```

You may refer to [Flink's document for Hive
connector](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/connectors/table/hive/overview/#supported-hive-versions)
for supported Hadoop & Hive versions.

### Format Code Style

FeatHub uses the following tools to maintain code quality:
- [Black](https://black.readthedocs.io/en/stable/index.html) to format Python code
- [flake8](https://flake8.pycqa.org/en/latest/) to check Python code style
- [mypy](https://mypy.readthedocs.io/en/stable/) to check type annotation

Before uploading pull requests (PRs) for review, format codes, check code
style, and check type annotations using the following commands:

```bash
# Format python code
$ python -m black ./python

# Check python code style
$ python -m flake8 --config=python/setup.cfg ./python

# Check python type annotation
$ python -m mypy --config-file python/setup.cfg ./python
```

## Roadmap

Here is a list of key features that we plan to support:

- [x] Support all FeatureView transformations with FlinkProcessor
- [x] Support all FeatureView transformations with LocalProcessor
- [x] Support all FeatureView transformations with SparkProcessor
- [x] Support common online and offline feature storages (e.g. Kafka, Redis, Hive, MySQL)
- [x] Support persisting feature metadata in MySQL
- [x] Support exporting pre-defined and user-defined feature metrics to Prometheus
- [ ] Support online transformation with feature service
- [ ] Support feature metadata exploration (e.g. definition, lineage, metrics) with FeatHub UI

## Contact Us

Chinese-speaking users are recommended to join the following DingTalk group for
questions and discussion. You need to join the "Apache Flink China" DingTalk
organization via
[this](https://wx-in-i.dingtalk.com/invite-page/weixin.html?bizSource=____source____&corpId=ding82d2a9eeaf9e30ff35c2f4657eb6378f&inviteCode=zmC5CSqct5jEXoi)
link first in order to join the following DingTalk Group.

<img src="docs/static/img/dingtalk.png" width="20%" height="auto">

English-speaking users can use this [invitation
link](https://join.slack.com/t/feathubworkspace/shared_invite/zt-1ik9wk0xe-MoMEotpCEYvRRc3ulpvg2Q)
to join our [Slack channel](https://feathub.slack.com/) for questions and
discussion.

We are actively looking for user feedback and contributors from the community.
Please feel free to create pull requests and open Github issues for feedback and
feature requests.

Come join us!


## Additional Resources
- [Documentation](docs/content): Our documentation provides guidance
on compute engines, connectors, expression language, and more. Check it out if
you need help getting started or want to learn more about FeatHub.
- [FeatHub Examples](https://github.com/flink-extended/feathub-examples): This
repository provides a wide variety of FeatHub demos that can be executed using
Docker Compose. It's a great resource if you want to try out FeatHub and see
what it can do.
- Tech Talks and Articles
  - DataFun 2023 ([slides](https://www.slideshare.net/DongLin1/feathubdatafun2023pptx))
  - Flink Forward Asia 2022 ([slides](https://www.slideshare.net/DongLin1/feathub), [video](https://www.bilibili.com/video/BV1714y1E7fQ/?spm_id_from=333.337.search-card.all.click), [article](https://mp.weixin.qq.com/s/ZFKRNaQODe0LwRT1nlwZgA))
