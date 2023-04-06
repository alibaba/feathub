# Apache Flink

The FlinkProcessor does feature ETL using Flink as the compute engine. In the
following sections we describe the deployment modes supported by FlinkProcessor
and the configuration keys accepted by each mode.

## Supported Versions

- Flink 1.15

## Deployment Mode
The Flink processor runs the Flink job in one of the following deployment modes:

- **Command-Line mode**. This mode should be used if you want to run the FeatHub
  program using Flink's [command-line
interface](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/cli/#command-line-interface).
- **Session mode**. This mode should be used if you want to run the FeatHub program
  in the Flink [session
mode](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/overview/#session-mode).
This mode runs Flink jobs in an already running Flink cluster.
- **Kubernetes Application mode**. This mode should be used if you want to run the
  FeatHub program in the Flink [application
mode](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/overview/#application-mode)
on a Kubernetes cluster. This mode creates a dedicated Flink cluster and runs
Flink jobs in this cluster.

Session mode is the default mode. User can specify the deployment mode with
configuration `deployment_mode`.

### Command-Line mode

When running in CLI mode, FeatHub itself will not submit the Flink job to a
Flink cluster. Users need to manually submit the FeatHub job as a Flink job
using the Flink [CLI tool](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/#command-line-interface).

A quickstart of how to submit a simple FeatHub job with CLI mode to a standalone Flink 
cluster can be found in this [document](../get-started/quickstarts/flink-cli-mode.md).

### Session mode

Session mode assumes that there is a running cluster and the Flink job is
submitted to the Flink cluster. User needs to specify the `master` configuration
with the address and the port where the JobManager runs.

Specifically, if `master` is configured to `"local"`, FlinkProcessor would set
up a Flink MiniCluster to execute the submitted job. This case can be used for
development and proof of concept.

The advantage of session mode is that you do not pay the overhead of spinning up a Flink
cluster for every submitted job. And users can convert the features to Pandas dataframe 
and use libraries in pandas ecosystem. The session mode is convenient for local testing 
and prototyping. The disadvantage is that it doesn't isolate resource between jobs, 
which means one misbehaving job can affect other jobs. You can refer to 
the [Flink Docs](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/overview/#session-mode)
for explanation of session mode. 

A quickstart of how to submit a simple FeatHub job with session mode to a standalone 
Flink cluster can be found in this [document](../get-started/quickstarts/flink-session-mode.md).

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

FeatHub provides a base Docker image to run the Flink job that compute the FeatHub 
features. User can modify the [Dockerfile](../../../docker/Dockerfile) to further customize 
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

In the following we describe the configuration keys accepted by the
configuration dict passed to the FlinkProcessor. Note that the accepted
configuration keys depend on the deployment_mode of the FlinkProcessor.

### Basic Configuration

These are the configuration keys accepted by all deployment modes.

| key             | Required | default | type   | Description                                                                              |
|-----------------|----------|---------|--------|------------------------------------------------------------------------------------------|
| deployment_mode | optional | session | String | The flink job deployment mode, it could be "cli", "session" or "kubernetes-application". |
| native.*                | optional | (none)         | String | Any key with the "native" prefix will be forwarded to the Flink job config after the "native" prefix is removed. For example, if the processor config has an entry "native.parallelism.default: 2", then the Flink job config will have an entry "parallelism.default: 2". |

### Session Mode Configuration

These are the extra configuration keys accepted when deployment_mode = "session":

| key          | Required | default | type    | Description                                                                                                                                |
|--------------|----------|---------|---------|--------------------------------------------------------------------------------------------------------------------------------------------|
| master | required | (none)  | String  | The Flink JobManager URL to connect to.                                                                                              |

### Kubernetes Application Mode Configuration

These are the extra configuration keys accepted when deployment_mode = "kubernetes-application":

| key                    | Required | default        | type   | Description                                                                                                                                                                                                                                                                                                                                                                                        |
|------------------------|----------|----------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| flink_home             | optional | (none)         | String | The path to the Flink distribution. If not specified, it uses the Flink's distribution in PyFlink.                                                                                                                                                                                                                                                                                                 |
| kubernetes.image       | optional | feathub:latest | String | The docker image to start the JobManager and TaskManager pod.                                                                                                                                                                                                                                                                                                                                      |
| kubernetes.namespace   | optional | default        | String | The namespace of the Kubernetes cluster to run the Flink job.                                                                                                                                                                                                                                                                                                                                      |
| kubernetes.config.file | optional | ~/.kube/config | String | The kubernetes config file is used to connector to the Kubernetes cluster.                                                                                                                                                                                                                                                                                                                         |
