# Prometheus

The PrometheusMetricStore supports reporting metrics to a Prometheus service
through [Prometheus Pushgateway](https://github.com/prometheus/pushgateway). 

## Metric reporting format

The metrics reported by PrometheusMetricStore generally match the default format
with the following modifications.

- It uses the label name "job" instead of "namespace" to hold the namespace
  value of the metric store.
- table_name is not only reported as a label of the metric, but also the
  grouping key of the job in Prometheus PushGateway.


## Configurations

The following describes the configurations required to set up a
PrometheusMetricStore instance.

| Key                | Required | Default | Type    | Description                                                  |
| ------------------ | -------- | ------- | ------- | ------------------------------------------------------------ |
| server_url         | Required | -       | String  | The PushGateway server URL including scheme, host name, and port. |
| delete_on_shutdown | Optional | True    | Boolean | Whether to delete metrics from Prometheus when the job finishes. When set to true, Feathub will try its best to delete the metrics but this is not guaranteed. |

## Examples

Here is an example that creates a FeathubClient that reports metrics to
Prometheus.

```python
client = FeathubClient(
    {
        "processor": {
            "type": "local",
        },
        "online_store": {
            "types": ["memory"],
            "memory": {},
        },
        "registry": {
            "type": "local",
            "local": {
                "namespace": "default",
            },
        },
        "feature_service": {
            "type": "local",
            "local": {},
        },
      	"metric_store": {
            "type": "prometheus",
            "report_interval_sec": 5,
            "prometheus": {
                "server_url": "localhost:8080",
                "delete_on_shutdown": False,
            },
        }
    }
)
```