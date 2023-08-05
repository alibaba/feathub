# Built-in Metrics

Below are Feathub's built-in metrics's metric types, their parameters and their
exposed tags.

## Count

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

## Ratio

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

