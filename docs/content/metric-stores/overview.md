# Overview

A MetricStore provides properties to set the metrics of a Feathub job into an
external metric service. A metric refers to a statistic of a characteristic of a
feature.

## Metric store configurations

Below are common configurations shared by different metric store
implementations. The document of each metric store contains its specific
configurations.

| Key                 | Required | Default   | Type    | Description                                                  |
| ------------------- | -------- | --------- | ------- | ------------------------------------------------------------ |
| type                | Required | -         | String  | The type of the metric store to use.                         |
| report_interval_sec | Optional | 2        | Float   | The interval in seconds to report metrics.                   |
| namespace           | Optional | "default" | String  | The namespace to report metrics to the metric store. Metrics within different namespace will not overwrite each other. |

## Defining metrics

Feathub supports defining metrics at feature's granularity. Below is an example
of defining a metric for a feature.

```python
f_total_cost = Feature(
    name="total_cost",
    transform=SlidingWindowTransform(
        expr="cost",
        agg_func="SUM",
        group_by_keys=["name"],
        window_size=timedelta(days=2),
        step_size=timedelta(days=1),
    ),
    metrics=[
    	Count(
          filter_expr="> 100",
          window_size=timedelta(days=1),
      ),
    	Ratio(
          filter_expr="IS NULL",
          window_size=timedelta(hours=1),
      ),
    ],
)
```

Then when the FeatureView that hosts the above feature is being materialized,
the related metrics will also be reported during the materialization process.

By default, Feathub would only report the metrics directly defined in the
FeatureView to be materialized. If the `keep_source_metrics` parameter is
enabled when creating the feature view as follows, FeatHub will recursively
enumerate the source feature view of this and every upstream feature view whose
`keep_source_fields == true`, and report metrics defined in those feature views.

```python
feature_view_0 = ...

feature_view_1 = SlidingFeatureView(
    name="feature_view_1",
    source=feature_view_0,
    features=[f_total_cost], # A feature containg metrics.
)

feature_view_2 = DerivedFeatureView(
    name="feature_view_2",
    source=feature_view_1,
    features=["name"],
    keep_source_metrics=True, # enable keep_source_metrics here.
)

# This will report all metrics defined in feature_view_1 and feature_view_2. But
# as feature_view_1 does not set keep_source_metrics to True, metrics defined in
# feature_view_0 will not be reported.
self.client.materialize_features(features, sink=...).wait()
```

## Metric reporting format

Metrics reported by metric stores will have the following format by default.
Some metric stores might override the default format, and please check the
document of each metric store for details.

- Metric name: `"{namespace}_{feature_name}_{metric_type}"`
  - namespace: The namespace of the metric store.
  - feature_name: The name of the host feature.
  - metric_type: The type of the metric.
- Metric tags:
  - namespace: The namespace of the metric store.
  - table_name: The name of the sink where the host features would be written
    to.
  - feature_name: The name of the host feature.
  - other metric-specific tags.

## Built-in metrics

Below are Feathub's built-in metrics's metric types, their parameters and their
exposed tags.

### Count

Count is a metric that shows the number of features. It has the following
parameters:

- filter_expr: Optional with None as the default value. If it is not None, it
  represents a partial FeatHub expression which evaluates to a boolean value.
  The partial Feathub expression should be a binary operator whose left child is
  absent and would be filled in with the host feature name. For example, "IS
  NULL" will be enriched into "{feature_name} IS NULL". Only features that
  evaluate this expression into True will be considered when computing the
  metric.
- window_size: Optional with 0 as the default value. The time range to compute
  the metric. It should be zero or a positive time span. If it is zero, the
  metric will be computed from all feature values that have been processed since
  the Feathub job is created.

It exposes the following metric-specific tags:

- metric_type: "count"
- filter_expr: The value of the filter_expr parameter.
- window_size_sec: The value of the window_size parameter in seconds.

### Ratio

Ratio is a metric that shows the proportion of the number features that meets
filter_expr to the number of all features. It has the following parameters:

- filter_expr: A partial FeatHub expression which evaluates to a boolean value.
  The partial Feathub expression should be a binary operator whose left child is
  absent and would be filled in with the host feature name. For example, "IS
  NULL" will be enriched into "{feature_name} IS NULL". Only features that
  evaluate this expression into True will be considered when computing the
  metric.
- window_size: Optional with 0 as the default value. The time range to compute
  the metric. It should be zero or a positive time span. If it is zero, the
  metric will be computed from all feature values that have been processed since
  the Feathub job is created.

It exposes the following metric-specific tags:

- metric_type: "ratio"
- filter_expr: The value of the filter_expr parameter.
- window_size_sec: The value of the window_size parameter in seconds.

