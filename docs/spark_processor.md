# Spark Processor

The SparkProcessor does feature ETL using Spark as the processing engine. In the
following sections we describe the deployment modes supported by SparkProcessor
and the configuration keys accepted by each mode.

## Deployment Mode

FeatHub's SparkProcessor only supports Spark's client mode. When running in
client mode, the Spark driver component of the spark application will run on the
machine from where the job submitted.

## Configurations

In the following we describe the configuration keys accepted by the
configuration dict passed to the SparkProcessor.

| key             | Required | default | type   | Description                                                                              |
|-----------------|----------|---------|--------|------------------------------------------------------------------------------------------|
| master | Required | (None) | String | The Spark master URL to connect to. Check [this link](https://spark.apache.org/docs/3.3.1/submitting-applications.html#master-urls) for valid url formats. |
| native.*                | optional | (none)         | String | Any key with the "native" prefix will be forwarded to the Spark Session config after the "native" prefix is removed. For example, if the processor config has an entry "native.spark.default.parallelism": 2, then the Spark Session config will have an entry "spark.default.parallelism": 2. |

