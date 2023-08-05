# FeatureView - A Table of Features

A `FeatureView` provides metadata to derive a table of feature values from
other tables. FeatHub currently supports the following types of FeatureViews.

- [DerivedFeatureView](https://github.com/alibaba/feathub/blob/master/python/feathub/feature_views/derived_feature_view.py)
  derives features by applying the given transformations on an existing table.
  It supports per-row transformation, over window transformation and table join.
  It does not support sliding window transformation.
- [SlidingFeatureView](https://github.com/alibaba/feathub/blob/master/python/feathub/feature_views/sliding_feature_view.py)
  derives features by applying the given transformations on an existing table.
  It supports per-row transformation and sliding window transformation. It does
  not support join or over window transformation.
- [OnDemandFeatureView](https://github.com/alibaba/feathub/blob/master/python/feathub/feature_views/on_demand_feature_view.py)
  derives features by joining online request with features from tables in online
  feature stores. It supports per-row transformation and join with tables in
  online stores. It does not support over window transformation or sliding window
  transformation.
- [SqlFeatureView](https://github.com/alibaba/feathub/blob/master/python/feathub/feature_views/sql_feature_view.py)
  derives features by evaluating a given SQL statement.  Currently, its
  semantics depends on the processor used during deployment. We plan to make it
  processor-agnostic in the future to ensure consistent semantics regardless of
  processor choice.

`FeatureView` provides APIs to specify and access `Feature`s. Each `Feature` is
defined by the following metadata:
- `name`: a string that uniquely identifies this feature in the parent table.
- `dtype`: the data type of this feature's values.
- `transform`: A declarative definition of how to derive this feature's values.
- `keys`: an optional list of strings, corresponding to the names of fields in
  the parent table necessary to interpret this feature's values. If it is
  specified, it is used as the join key when FeatHub joins this feature onto
  another table.

# Transformation - Declarative Definition of Feature Computation

A `Transformation` defines how to derive a new feature from existing features.
FeatHub currently supports the following types of Transformations.

- [ExpressionTransform](https://github.com/alibaba/feathub/blob/master/python/feathub/feature_views/transforms/expression_transform.py)
  derives feature values by applying FeatHub expression on one row of the
  parent table at a time. The FeatHub expression language is a declarative
  language, sharing a syntax and grammar reminiscent of the SQL SELECT clause.
  See [here](./) for a comprehensive list of built-in data types, functions
  and operators.
- [OverWindowTransform](https://github.com/alibaba/feathub/blob/master/python/feathub/feature_views/transforms/over_window_transform.py)
  derives feature values by applying FeatHub expression and aggregation function
  on multiple rows of a table at a time.
- [SlidingWindowTransform](https://github.com/alibaba/feathub/blob/master/python/feathub/feature_views/transforms/sliding_window_transform.py)
  derives feature values by applying FeatHub expression and aggregation function
  on multiple rows in a sliding window.
- [JoinTransform](https://github.com/alibaba/feathub/blob/master/python/feathub/feature_views/transforms/join_transform.py)
  derives feature values by joining parent table with a feature from another
  table.
- [PythonUdfTransform](https://github.com/alibaba/feathub/blob/master/python/feathub/feature_views/transforms/python_udf_transform.py)
  derives feature values by applying a Python UDF on one row of the parent table
  at a time.


