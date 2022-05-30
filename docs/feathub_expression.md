# Feathub Expression Language

Feathub expression language is a declarative language with built-in functions.
It can be used in the `ExpressionTransform` to describe how to derive a new
feature value from existing features' values for each row in a table.

In the following, we describe the built-in functions supported by the Feathub
expression language.

## Arithmetic Functions

Arithmetic functions take numeric values as inputs and outputs a numeric value.

| Function | Description |
| --- | --- |
| x + y | Returns `x + y`. Returns NULL if `x` or `y` is NULL. |
| x - y | Returns `x - y`. Returns NULL if `x` or `y` is NULL. |
| x * y | Returns `x * y`. Returns NULL if `x` or `y` is NULL. |
| x / y | Returns `x / y`. Returns NULL if `x` or `y` is NULL. |


## Comparison Functions

Comparison functions take numeric values as inputs and outputs a boolean value.

| Function | Description |
| --- | --- |
| x > y | Returns TRUE iff `x > y`. Returns NULL if in case of error. |
| x >= y | Returns TRUE iff `x >= y`. Returns NULL if in case of error. |
| x < y | Returns TRUE iff `x < y`. Returns NULL if in case of error. |
| x <= y | Returns TRUE iff `x <= y`. Returns NULL if in case of error. |
| x == y | Returns TRUE iff `x` is equal to `y`. Returns NULL if in case of error. |
| x <> y | Returns TRUE iff `x` is not equal to `y`. Returns NULL if in case of error. |

## Type Conversion Functions

| Function | Description |
| --- | --- |
| cast_float(x) | Returns a new value being casted to a float. Returns NULL if in case of error. |
| cast_int(x) | Returns a new value being casted to an integer. Returns NULL if in case of error. |

## Temporal Functions

| Function | Description |
| --- | --- |
| unix_timestamp(sring1) | Converts dateime string string1 in the format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds). Returns NULL if in case of error. |


