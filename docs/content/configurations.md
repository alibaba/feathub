# Common Configurations

## SlidingFeatureView

The following configurations are supported by `SlidingFeatureView`. They can be
specified either via the `extra_props` parameter when constructing
`SlidingFeatureView`, or via the `props` parameter of `FeatureView#build`.


| Key      | Required | Default | Type    | Description                                                  |
| -------- | -------- | ------- | ------- | ------------------------------------------------------------ |
| sdk.sliding_feature_view.enable_empty_window_output | Optional | True | Boolean | If it is True, when the sliding window becomes emtpy, it outputs zero value for aggregation function SUM and COUNT, and output None for other aggregation function. If it is False, the sliding window doesn't output anything when the sliding window becomes empty. |
| sdk.sliding_feature_view.skip_same_window_output    | Optional | True  | Boolean | If it is True, the sliding feature view only outputs when the result of the sliding window changes. If it is False, the sliding feature view outputs at every step size even if the result of the sliding window doesn't change. |





