# Protobuf Format

The Protocol Buffers [Protobuf](https://developers.google.com/protocol-buffers) format 
allows you to read and write Protobuf data, based on Protobuf generated classes.

## Format Options

| key                 | Required | default | type    | Description                                                                                                                                                                                       |
|---------------------|----------|---------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| protobuf.jar_path   | Required | (none)  | string  | The path to the jar that contains the Protobuf generated classes.                                                                                                                                 |
| protobuf.class_name | Required | (none)  | string  | The full name of a Protobuf generated class. The name must match the message name in the proto definition file. $ is supported for inner class names, like 'com.exmample.OuterClass$MessageClass' |
| ignore_parse_error  | Optional | True    | boolean | Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors.                                                                                              |

## How to create a table with Protobuf format

Here is an example to create a table using the Kafka connector and Protobuf format.

Below is the proto definition file.

```protobuf
syntax = "proto3";

package org.feathub.proto;
option java_package = "org.feathub.proto";
option java_multiple_files = true;

message KafkaTestMessage {
  int64 id = 1;
  int64 val = 2;
  string ts = 3;
}
```

1. Use [protoc](https://developers.google.com/protocol-buffers/docs/javatutorial#compiling-your-protocol-buffers)
   command to compile the .proto file to java classes.
2. Compile and package the classes. 
   [feathub-test-proto](../../../../java/feathub-test-proto) is an example that use 
   maven to compile and package the .proto file.

Then you can use Feathub connector to use the protobuf format.

```python
schema = (
    Schema.new_builder()
    .column("id", types.Int64)
    .column("val", types.Int64)
    .column("ts", types.String)
    .build()
)

source = KafkaSource(
    "kafka_source",
    bootstrap_server="bootstrap_server",
    topic="topic_name",
    key_format=None,
    value_format="protobuf",
    schema=schema,
    consumer_group="test-group",
    keys=["id"],
    timestamp_field="ts",
    timestamp_format="%Y-%m-%d %H:%M:%S",
    value_format_config={
        "protobuf.jar_path": "jar_path",  # The path to the jar packaged above.
        "protobuf.class_name": "org.feathub.proto.KafkaTestMessage"
    },
)
```

## Data Type Mapping

The following table lists the type mapping from Feathub type to Protobuf type.

| Feathub type | Protobuf type |
|--------------|---------------|
| Bytes        | bytes         |                               
| String       | string        |                               
| Int32        | int32         |                               
| Int64        | int64         |                               
| Float32      | float         |                               
| Float64      | double        |                              
| Bool         | bool          |                               
| VectorType   | repeated      |


## Null Values

As protobuf does not permit null values in maps and array, we need to auto-generate 
default values when converting from Flink Rows to Protobuf.

| Protobuf Data Type             | Default Value                |
|--------------------------------|------------------------------|
| int32 / int64 / float / double | 0                            |
| string                         | ""                           |
| bool                           | false                        |
| enum                           | first element of enum        |
| binary                         | ByteString.EMPTY             |
| message                        | MESSAGE.getDefaultInstance() |