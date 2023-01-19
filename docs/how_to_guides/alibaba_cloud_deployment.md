# Deploy Feathub Job to Alibaba Cloud Realtime Compute for Apache Flink(实时计算Flink版)

In this document, we will show you the steps to deploy a simple Feathub job that 
consumes data from the Flink datagen connector, computes some features, and prints out 
the result to the [Alibaba Cloud Realtime Compute for Apache Flink(实时计算Flink版)](https://help.aliyun.com/document_detail/110778.html).

## Prerequisite

- An up and running instance of [Alibaba Cloud Realtime Compute for Apache Flink(实时计算Flink版).](https://help.aliyun.com/document_detail/110778.html)

## Package Feathub Python Dependencies

We provide the script to package a zip that contains the Python dependencies to run
Feathub job in Alibaba Cloud Realtime Compute for Apache Flink. You can run the 
following command to build the zip. Note that you need to have docker installed to run 
the script.

```bash
$ bash tools/vvp-dep/build-vvp-dep.sh
```

The dependencies zip will be available at `tools/cli-deps/deps.zip`. 

You can modify the script to include any additional Python dependencies to the 
`deps.zip`. Or you can package your own zip that only includes the additional 
dependencies and add it to the Python Library when creating the PyFlink Job in the next
step.

## Create a PyFlink job

1. Upload and use the python script at `python/feathub/examples/streaming_average_flink_cli.py`
   as the entrypoint of the job.
2. Upload and add the `deps.zip` and any additional python dependencies built in the 
   last step to the Python Library.
3. Publish and start the job.

For additional information regarding PyFlink job development on Alibaba Cloud Realtime 
Compute for Apache Flink you can refer to [here](https://help.aliyun.com/document_detail/207346.html).

## Result

When the job is running, you can check the output at the TaskManager log.