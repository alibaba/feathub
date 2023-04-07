# Window Aggregations

FeatHub can use `OverWindowTransform` and `SlidingWindowTransform` to describe
how to derive a feature value by applying FeatHub expression and aggregation
function on multiple rows. In the following, we provide example usages of these
window transforms and the supported aggregation functions.

## OverWindowTransform

`OverWindowTransform` derives a feature value by applying FeatHub expression and
aggregation function on multiple rows of a table at a time. It can be used in
`DerivedFeatureView`.

Below is an example usage of `OverWindowTransform`.

```python
f_total_cost = Feature(
    name="total_cost",
    transform=OverWindowTransform(
        expr="cost",
        agg_func="SUM",
        group_by_keys=["name"],
        window_size=timedelta(days=2),
    ),
)

features = DerivedFeatureView(
    name="feature_view",
    source=source,
    features=[
        f_total_cost,
    ],
    keep_source_fields=False,
)
```

## SlidingWindowTransform

`SlidingWindowTransform` derives a feature value by applying FeatHub expression
and aggregation function on multiple rows in a sliding window. It can be used in
`SlidingFeatureView`.

Below is an example usage of `SlidingWindowTransform`.

```python
f_total_cost = Feature(
    name="total_cost",
    transform=SlidingWindowTransform(
        expr="cost",
        agg_func="SUM",
        window_size=timedelta(days=3),
        group_by_keys=["name"],
        limit=2,
        step_size=timedelta(days=1),
    ),
)

features = SlidingFeatureView(
  name="features",
  source=source,
  features=[f_total_cost],
)
```

## Aggregation Functions

In the following, we describe the built-in aggregation functions supported by
FeatHub window transforms.

| Function     | Description                                                  |
| ------------ | ------------------------------------------------------------ |
| AVG          | Returns the average (arithmetic mean) of an expression or column across input rows.         |
| SUM          | Returns the sum of an expression or column across input rows.                               |
| MAX          | Returns the maximum value of an expression or column across input rows.                     |
| MIN          | Returns the minimum value of an expression or column across input rows.                     |
| FIRST_VALUE  | Returns the first value (ordered by timestamp) of an expression or column across input rows. |
| LAST_VALUE   | Returns the last value (ordered by timestamp) of an expression or column across input rows. |
| ROW_NUMBER   | Assigns a unique, sequential number to each row, starting with one, to the input rows within the window partition. |
| COUNT        | Returns the number of input rows.                                                           |
| VALUE_COUNTS | Returns a frequency map containing the counts of unique values of an expression or column across input rows. |


