# Feathub Expression Language

Feathub expression language is a declarative language with built-in functions.
It can be used in the `ExpressionTransform` to describe how to derive a new
feature value from existing features' values for each row in a table.

In the following, we describe the built-in functions supported by the Feathub
expression language.

## Reserved Keywords
Feathub expression has the following string reserved as keywords:

AS, BIGINT, BOOLEAN, BYTES, CAST, DOUBLE, FALSE, FLOAT, INTEGER, STRING, TIMESTAMP, 
TRUE, TRY_CAST

Note that the keywords are case-insensitive, e.g., BIGINT, bigint, or Bigint are all 
the same. We suggest user avoiding using the reserved keywords as variable names. 
However, if you have to use a reserved keyword as a variable name, make sure to 
surround it with backticks (e.g. \`integer\`, \`cast\`). 

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

## Logical Functions

| Function        | Description                                     |
|-----------------|-------------------------------------------------|
| bool1 OR bool2  | Returns TRUE if bool1 is TRUE or bool2 is TRUE. |
| bool1 AND bool2 | Returns TRUE if bool1 and bool2 are both TRUE.  |

## Type Conversion Functions

| Function             | Description                                                                               |
|----------------------|-------------------------------------------------------------------------------------------|
| CAST(x AS DTYPE)     | Returns a new value being casted to a the given DTYPE. Throws exception in case of error. |
| TRY_CAST(x AS DTYPE) | Returns a new value being casted to a the given DTYPE. Returns NULL if in case of error.  |

### Data Types

Feathub expression supports the following data types: 

BYTES, STRINGS, INTEGER, BIGINT, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP

The matrix below describes the supported cast between any two data types, where "Y" 
means supported, "N" means unsupported, and "!" means fallible. If the cast is fallible 
and the provided input is not valid, CAST will fail the job and TRY_CAST will return 
NULL. 

| Input\Target | BYTES | STRINGS | INTEGER | BIGINT | FLOAT | DOUBLE | BOOLEAN | TIMESTAMP | 
|--------------|-------|---------|---------|--------|-------|--------|---------|-----------|
| BYTES        | Y     | Y       | N       | N      | N     | N      | N       | N         |
| STRINGS      | N     | Y       | !       | !      | !     | !      | !       | !         |
| INTEGER      | N     | Y       | Y       | Y      | Y     | Y      | Y       | N         |
| BIGINT       | N     | Y       | Y       | Y      | Y     | Y      | Y       | N         |
| FLOAT        | N     | Y       | Y       | Y      | Y     | Y      | Y       | N         |
| DOUBLE       | N     | Y       | Y       | Y      | Y     | Y      | Y       | N         |
| BOOLEAN      | N     | Y       | Y       | Y      | Y     | Y      | Y       | N         |
| TIMESTAMP    | N     | Y       | N       | N      | N     | N      | N       | Y         |

## Temporal Functions

| Function               | Description                                                                                                                         |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| UNIX_TIMESTAMP(sring1) | Converts dateime string string1 in the format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds). Returns NULL if in case of error. |


