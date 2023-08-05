# Built-in Optimizations

FeatHub offers several built-in optimizations that improve performance when
using Apache Flink as the compute engine. By leveraging these optimizations,
FeatHub can deliver better performance than using Apache Flink directly.

In the following section, we describe some of the key optimizations and their
impact on performance for selected use cases.

## Reusing the Same Data History for Multiple Sliding Window Features

**Target Feature Pattern**: Multiple `SlidingWindowTransform` features are
defined in the same `SlidingFeatureView` and those features only differ in their
`window_size`. For example, a user may want to compute the count of clicks per
`user_id` in the last 1 hour, 3 hours, and 6 hours.

A naive Flink job would create a separate operator for each feature, with each
operator needing to preserve the entire data history of the corresponding window
in the state-backend to expire old data as the window slides over time. For the
example described above, this would require persisting and loading a total of 10
hours' worth of data to compute feature values.

FeatHub takes advantage of the overlapping data in these windows and only
persists the data for the largest window. This data can then be reused to
compute all of the other features. In the example above, FeatHub would only need
to persist 6 hours' worth of data.

This optimization can significantly reduce storage costs and memory usage for
the Flink job.

See
[here](https://github.com/flink-extended/feathub-examples/blob/master/flink-sliding-feature-view-benchmark/main.py)
for an example program that applies this optimization.


## Outputting Sliding Window Features Only When They Change

**Target Feature Pattern**: The input data for a sliding window feature is
sparse, with only a few input records during a long window period. For example,
the window size may be 6 hours with a step size of 1 second, and there are only
10 input records in the last 6 hours, meaning that the feature value changes at
most 10 times during this period.

A naive Flink job would use Flink's built-in sliding window API to compute the
feature and output one record at each step. For the example described above, a
total of 21,600 records would be outputted during the 6-hour window.

FeatHub allows the job to output feature values only for those steps where the
value differs from the previous feature value. This means that FeatHub only
needs to output 10 records in the example described above. This optimization can
significantly reduce I/O costs and network bandwidth usage.

This optimization is enabled by default. See
[SlidingFeatureView](../configurations.md#slidingfeatureview) configuration for
more details.

