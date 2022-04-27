# OTel Metrics String Processor 

This is a processor that serializes a collection of `ExportMetricsServiceRequest` sent from [otel-metrics-source](../dataPrepper-plugins/otel-metrics-source) into a collection of string records.

## Usages
Example `.yaml` configuration
```
processor:
    - otel_metrics_raw_processor
```

## Configuration
It is possible to create explicit representations of histogram buckets and their boundaries. This feature can be controlled with the following parameters:

```yaml
  processor:
    - otel_metrics_raw_processor:
        calculate_histogram_buckets: true
        calculate_exponential_histogram_buckets: true
```
There are two parameters: `calculate_histogram_buckets` and `calculate_exponential_histogram_buckets`.
If a parameter is not provided it defaults to `false`.

If `calculate_histogram_buckets` is set to `true`, the following JSON will be added to every histogram JSON:

```json
 "buckets": [
    {
      "min": 0.0,
      "max": 5.0,
      "count": 2
    },
    {
      "min": 5.0,
      "max": 10.0,
      "count": 5
    }
  ]
```

Each array element describes one bucket. Each bucket contains the lower boundary, upper boundary and its value count.
This is an explicit form of the more dense OpenTelemetry representation that is already part of the JSON output created by this plugin:

```json
 "explicitBounds": [
    5.0,
    10.0
  ],
   "bucketCountsList": [
    2,
    5
  ]
```


If `calculate_exponential_histogram_buckets` is set to `true`, the following JSON will be added to every histogram JSON:
```json

    "negativeBuckets": [
        {
        "min": 0.0,
        "max": 5.0,
        "count": 2
        },
        {
        "min": 5.0,
        "max": 10.0,
        "count": 5
        }
    ],
...
    "positiveBuckets": [
        {
        "min": 0.0,
        "max": 5.0,
        "count": 2
        },
        {
        "min": 5.0,
        "max": 10.0,
        "count": 5
        }
    ],
```

Again, this is a more explicit form of the dense OpenTelemetry representation which consists of negative and positive buckets along with
a scale parameter, offset and list of bucket counts:
```json
    "negative": [
        1,
        2,
        3
    ],
    "positive": [
        1,
        2,
        3
    ],
    "scale" : -3,
    "negativeOffset" : 0,
    "positiveOffset : 1
```

## Metrics
This plugin uses all common metrics in [AbstractProcessor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/com/amazon/dataprepper/model/processor/AbstractProcessor.java), and does not currently introduce custom metrics.

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
