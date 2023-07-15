# Data Types

## Primitive Data Types

FeatHub supports the following primitive data types: 

BYTES, STRING, INTEGER, BIGINT, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP

The matrix below describes the supported cast between any two data types, where "Y" 
means supported, "N" means unsupported, and "!" means fallible. If the cast is fallible 
and the provided input is not valid, CAST will fail the job and TRY_CAST will return 
NULL. 

| Input\Target | BYTES | STRING | INTEGER | BIGINT | FLOAT | DOUBLE | BOOLEAN | TIMESTAMP | 
|--------------|-------|--------|---------|--------|-------|--------|---------|-----------|
| BYTES        | Y     | Y      | N       | N      | N     | N      | N       | N         |
| STRING       | N     | Y      | !       | !      | !     | !      | !       | !         |
| INTEGER      | N     | Y      | Y       | Y      | Y     | Y      | Y       | N         |
| BIGINT       | N     | Y      | Y       | Y      | Y     | Y      | Y       | N         |
| FLOAT        | N     | Y      | Y       | Y      | Y     | Y      | Y       | N         |
| DOUBLE       | N     | Y      | Y       | Y      | Y     | Y      | Y       | N         |
| BOOLEAN      | N     | Y      | Y       | Y      | Y     | Y      | Y       | N         |
| TIMESTAMP    | N     | Y      | N       | N      | N     | N      | N       | Y         |


## Composite Data Types

FeatHub supports the following composite data types:

- VECTOR, which represents a vector of elements of the same type. Element type
  can also be a composite type (e.g. map).
- MAP, which represents a map of key/value pairs. Key and value can be composite
  type (e.g. vector).


# Reserved Keywords
FeatHub expression has the following string reserved as keywords:

AS, BIGINT, BOOLEAN, BYTES, CAST, DOUBLE, FALSE, FLOAT, INTEGER, STRING, TIMESTAMP, 
TRUE, TRY_CAST

Note that the keywords are case-insensitive, e.g., BIGINT, bigint, or Bigint are all 
the same. We suggest user avoiding using the reserved keywords as variable names. 
However, if you have to use a reserved keyword as a variable name, make sure to 
surround it with backticks (e.g. \`integer\`, \`cast\`). 
