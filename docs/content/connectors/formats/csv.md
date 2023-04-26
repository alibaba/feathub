# CSV Format

The CSV format allows to read and write CSV data based on an CSV schema. The CSV schema 
is derived from table schema.

## Format Options

| key                | Required | default | type    | Description                                                                                          |
|--------------------|----------|---------|---------|------------------------------------------------------------------------------------------------------|
| ignore_parse_error | Optional | True    | boolean | Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors. |


## Data Type Mapping

The following table lists the type mapping from Feathub type to CSV type.

| Feathub type | Csv type                      |
|--------------|-------------------------------|
| Bytes        | string with encoding: base64  |
| String       | string                        |
| Int32        | number                        |
| Int64        | number                        |
| Float32      | number                        |
| Float64      | number                        |
| Bool         | boolean                       |
| Timestamp    | string with format: date-time |
| VectorType   | array                         |