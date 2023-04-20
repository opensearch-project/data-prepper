# OTel Metrics String Processor 

This is a processor that serializes a collection of `ExportMetricsServiceRequest` sent from [otel-metrics-source](../dataPrepper-plugins/otel-metrics-source) into a collection of string records.

> Note: `otel_metrics_raw_processor` processor has been renamed to `otel_metrics`. You can use either name for now but the support for `otel_metrics_raw_processor` will be removed in major version 3.0.

## Usages
Example `.yaml` configuration
```
processor:
    - otel_metrics_raw_processor
```

## Configurations
It is possible to create explicit representations of histogram buckets and their boundaries. This feature can be controlled with the following parameters:

```yaml
  processor:
    - otel_metrics_raw_processor:
        calculate_histogram_buckets: true
        calculate_exponential_histogram_buckets: true
        exponential_histogram_max_allowed_scale: 10
        flatten_attributes: false
```

There are three possible parameters: `calculate_histogram_buckets`, `calculate_exponential_histogram_buckets` and `exponential_histogram_max_allowed_scale`
If `calculate_histogram_buckets` and `calculate_exponential_histogram_buckets` are not provided they default to `false`. 
If `exponential_histogram_max_allowed_scale` is not provided it defaults to 10.

If `calculate_histogram_buckets` is not set to `false`, the following JSON will be added to every histogram JSON:
If `flatten_attributes` is set to `false`, the json string format of the metrics will keep the attributes field as is, and if it is set to `true`, the fleds in attributes field are put in the parent json object. Default is `true`

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


If `calculate_exponential_histogram_buckets` is not set to `false`, the following JSON will be added to every histogram JSON:
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
    "positiveOffset" : 1
```

The `exponential_histogram_max_allowed_scale` parameter defines the maximum allowed scale for the exponential histogram. Increasing this parameter will increase potential
memory consumption. See [the spec](https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto) for more information on exponential histograms and their computational complexity.

All exponential histograms that have a scale that is above the configured parameter (by default, 10) will be discarded and logged with error level.
**Note**: the absolute scale value is used for comparison, so a scale of -11 will be treated equally to 11 and thus exceed the configured value of 10 - and be discarded.

## Metrics
This plugin uses all common metrics in [AbstractProcessor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/org/opensearch/dataprepper/model/processor/AbstractProcessor.java), and does not currently introduce custom metrics.

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
