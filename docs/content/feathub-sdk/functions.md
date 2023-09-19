- [Built-in Operators](#built-in-operators)
  * [Arithmetic Operators](#arithmetic-operators)
  * [Comparison Operators](#comparison-operators)
  * [Logical Operators](#logical-operators)
  * [Miscellaneous Operators](#miscellaneous-operators)
- [Built-in Functions](#built-in-functions)
  * [CASE](#case)
  * [CONCAT](#concat)
  * [CONCAT_WS](#concat-ws)
  * [JSON_STRING](#json-string)
  * [LOWER](#lower)
  * [MAP](#map)
  * [Type Conversion Functions](#type-conversion-functions)
  * [UNIX_TIMESTAMP](#unix-timestamp)

## Built-in Operators

### Arithmetic Operators

Arithmetic functions take numeric values as inputs and outputs a numeric value.

| Operator | Description |
| -------- | --- |
| x + y    | Returns `x + y`. Returns NULL if `x` or `y` is NULL. |
| x - y    | Returns `x - y`. Returns NULL if `x` or `y` is NULL. |
| x * y    | Returns `x * y`. Returns NULL if `x` or `y` is NULL. |
| x / y    | Returns `x / y`. Returns NULL if `x` or `y` is NULL. |


### Comparison Operators

Comparison functions take numeric values as inputs and outputs a boolean value.

| Operator      | Description                                                              |
|---------------|--------------------------------------------------------------------------|
| x > y         | Returns TRUE iff `x > y`. Returns NULL in case of error.                 |
| x >= y        | Returns TRUE iff `x >= y`. Returns NULL in case of error.                |
| x < y         | Returns TRUE iff `x < y`. Returns NULL in case of error.                 |
| x <= y        | Returns TRUE iff `x <= y`. Returns NULL in case of error.                |
| x = y         | Returns TRUE iff `x` is equal to `y`. Returns NULL in case of error.     |
| x <> y        | Returns TRUE iff `x` is not equal to `y`. Returns NULL in case of error. |
| x IS NULL     | Returns TRUE iff `x` is NULL.                                            |
| x IS NOT NULL | Returns TRUE iff `x` is not NULL.                                        |

### Logical Operators

| Operator        | Description                                     |
|-----------------|-------------------------------------------------|
| bool1 OR bool2  | Returns TRUE if bool1 is TRUE or bool2 is TRUE. |
| bool1 AND bool2 | Returns TRUE if bool1 and bool2 are both TRUE.  |

### Miscellaneous Operators

| Operator | Description                                                         |
| -------- | --------------------------------------------------------------------|
| x[y]     | Returns the value corresponding to the key `y` in the given map `x` |


## Built-in Functions

### CASE

`CASE WHEN expr1 THEN expr2 [WHEN expr3 THEN expr4]* [ELSE expr5] END` - When
`expr1` = true, returns `expr2`; else when `expr3` = true, returns `expr4`; else
returns `expr5`.

Arguments:

- `expr1`, `expr3` - the branch condition expressions should all be boolean
  type.
- `expr2`, `expr4`, `expr5` - the branch value expressions and else value
  expression should all be same type or coercible to a common type.

### CONCAT

`CONCAT(string1, string2,… )` - Returns a string that concatenates string1,
string2, ….E.g., CONCAT(‘AA’, ‘BB’, ‘CC’) returns “AABBCC”. Non-string
parameters would first be cast to string before concatenation.

### CONCAT_WS

`CONCAT_WS(string1, string2, string3,…)` - Returns a string that concatenates
string2, string3, … with a separator string1. The separator is added between the
strings to be concatenated. Non-string parameters would first be cast to string
before concatenation.

### JSON_STRING

`JSON_STRING(value)` - Returns a JSON string containing the serialized value. If the 
value is NULL, the function returns NULL.

```SQL
-- NULL
JSON_STRING(CAST(NULL AS INTEGER))
-- '1'
JSON_STRING(1)
-- 'true'
JSON_STRING(TRUE)
-- '"Hello, World!"'
JSON_STRING('Hello, World!')
-- '{"a":1,"b":2}'
JSON_STRING(MAP("a", 1, "b", 2))
```

### LOWER

`LOWER(str)` - Returns `str` with all characters changed to lowercase.

### MAP

`MAP(key0, value0, key1, value1, ...)` -  Create a map from the given key/value pairs. 
The function expects to have at least two arguments and an even number of arguments. 
All the keys must be of the same type, and all the values must be of the same type.

### SIZE

`SIZE(expr)` -  Returns the size of a vector or a map. The function returns null
for null input.

### Type Conversion Functions

| Function                          | Description                                                                               |
|-----------------------------------|-------------------------------------------------------------------------------------------|
| CAST(x AS DTYPE)                  | Returns a new value being casted to a the given DTYPE. Throws exception in case of error. |
| TRY_CAST(x AS DTYPE)<sup>1</sup>, | Returns a new value being casted to a the given DTYPE. Returns NULL in case of error.     |

1. The availability of this function in SparkProcessor depends on the version of
   the Spark cluster. This function might only be available in some commercial
   distributions of Spark, instead of in the community versions.

### UNIX_TIMESTAMP

`UNIX_TIMESTAMP(timeExp[, fmt])` - Returns the UNIX timestamp of the given time
or NULL in case of error.

Arguments:

- `timeExp` - A string which will be returned as a UNIX timestamp.
- `fmt` - Date/time format pattern to follow. Default value is "%Y-%m-%d
  %H:%M:%S". The format codes required by the C standard (1989 version) are
  regarded as valid format patterns. See [Python's strftime() and strptime()
  Behavior](https://docs.python.org/3.7/library/datetime.html#strftime-strptime-behavior)
  for a brief list of and introduction to these format patterns.

