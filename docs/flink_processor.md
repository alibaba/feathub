# Flink Processor

Flink processor is used to compute the Feathub features with Flink. In the following 
sections we describe how to use the Flink processor.

## Deployment Mode
The Flink processor runs the Flink job in one of the following mode:

- Command-Line mode
- Session mode
- Kubernetes Application mode

Session mode is the default mode. User can specify the deployment mode with 
configuration `deployment_mode`.

### Command-Line mode

When running in CLI mode, Feathub itself will not submit the Flink job to a
Flink cluster. Users need to manually submit the Feathub job as a Flink job
using the Flink [CLI tool](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/#command-line-interface).

A quickstart of how to submit a simple Feathub job with CLI mode to a standalone Flink 
cluster can be found in this [document](quickstarts/flink_processor_cli_quickstart.md).

### Session mode

Session mode assumes that there is a running cluster and the Flink job is submitted to 
the Flink cluster. User has to specify the `rest.address` and `rest.port` where the
JobManager runs.

The advantage of session mode is that you do not pay the overhead of spinning up a Flink
cluster for every submitted job. And users can convert the features to Pandas dataframe 
and use libraries in pandas ecosystem. The session mode is convenient for local testing 
and prototyping. The disadvantage is that it doesn't isolate resource between jobs, 
which means one misbehaving job can affect other jobs. You can refer to 
the [Flink Docs](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/overview/#session-mode)
for explanation of session mode. 

A quickstart of how to submit a simple Feathub job with session mode to a standalone 
Flink cluster can be found in this [document](quickstarts/flink_processor_session_quickstart.md).

### Kubernetes Application mode

When running in Kubernetes Application mode, a Flink cluster is created in a Kubernetes 
cluster per Flink job. This comes with better resource isolation than session mode. 
You can refer to 
the [Flink Docs](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/overview/#application-mode)
for explanation of application mode. 

**Note**: `Table#to_pandas` is not supported in Kubernetes Application mode.

You can refer to the [Flink Docs](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/native_kubernetes/#application-mode) 
for more explanation of Kubernetes Application mode.

#### Kubernetes Image
To run the Flink job in Kubernetes Application mode, a docker image is required.

Feathub provides a base Docker image to run the Flink job that compute the Feathub 
features. User can modify the [Dockerfile](../docker/Dockerfile) to further customize 
the image. You can refer to [here](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/#further-customization)
to learn how to customize the image. Then you can use the following command to build 
the image:

```bash
$ bash tools/build-feathub-flink-image.sh
```

The script builds the image with tag "feathub:latest". You can rename the tag of the 
image. After that, you need to push it to a Docker image registry where your kubernetes 
cluster can pull the image from. And you can specify the image with the 
configuration `kubernetes.image`.

## Configurations

Here is an exhaustive list of configurations for FlinkProcessor.

### Basic configuration
These are the configurations for the FlinkProcessor regardless of the deployment mode.

| key             | Required | default | type   | Description                                                                              |
|-----------------|----------|---------|--------|------------------------------------------------------------------------------------------|
| deployment_mode | optional | session | String | The flink job deployment mode, it could be "cli", "session" or "kubernetes-application". |

### Session Mode Configuration
These are the configurations for FlinkProcessor running in session mode.

| key          | Required | default | type    | Description                                   |
|--------------|----------|---------|---------|-----------------------------------------------|
| rest.address | required | (none)  | String  | The ip or hostname where the JobManager runs. |
| rest.port    | required | (none)  | Integer | The port where the JobManager runs.           |

### Kubernetes Application Mode Configuration
These are the configurations for FlinkProcessor running in Kubernetes Application mode.

| key                    | Required | default        | type   | Description                                                                                                                                                                                                                                                                                                                                                                                        |
|------------------------|----------|----------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| flink_home             | optional | (none)         | String | The path to the Flink distribution. If not specified, it uses the Flink's distribution in PyFlink.                                                                                                                                                                                                                                                                                                 |
| kubernetes.image       | optional | feathub:latest | String | The docker image to start the JobManager and TaskManager pod.                                                                                                                                                                                                                                                                                                                                      |
| kubernetes.namespace   | optional | default        | String | The namespace of the Kubernetes cluster to run the Flink job.                                                                                                                                                                                                                                                                                                                                      |
| kubernetes.config.file | optional | ~/.kube/config | String | The kubernetes config file is used to connector to the Kubernetes cluster.                                                                                                                                                                                                                                                                                                                         |
| flink.*                | optional | (none)         | String | This can set and pass arbitrary Flink configuration. The "flink" prefix in the key is removed before passing to Flink. For example, you can set the default parallelism via "flink.parallelism.default". Some configurations are ignored because they are overridden by Feathub. For example, the value of "flink.kubernetes.namespace" will be overridden by the value of "kubernetes.namespace". |
