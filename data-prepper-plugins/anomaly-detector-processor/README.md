# Anomaly Detector Processor

This is a processor that takes structured data and runs anomaly detection algorithm on user configured fields in the data. The data needs to be a number(integer or real) for the anomaly detection algorithm to detect anomalies. The anomaly detector processor supports the following ML algorithms to detect anomalies
 - random-cut-forest

## Basic Usage
To get started, create the following `pipeline.yaml`.
```yaml
ad-pipeline:
  source:
    http:
  processor:
    - anomaly_detector:
        mode: "random-cut-forest.metrics.v1"
        keys: ["latency"]
  sink:
    - stdout:
```

When run, the processor will parse the messages and extracts the values for the key "latency" and pass it through "random-cut-forest.metrics.v1" ML algorithm.

## Configuration
* `mode` - The ML algorithm to use
  * Default: `random-cut-forest.metrics.v1`
  * This is the only mode currently supported
* `keys` - List of keys to use as input to the ML algorithm
  * There is no default
  * This field cannot be empty.
* `shingleSize` - shingle size to be used in the ML algorithm
  * Default: 4
  * Range: 1 - 60
* `sampleSize` - sample size size to be used in the ML algorithm
  * Default: 256
  * Range: 100 - 2500
* `timeDecay` - time decay value to be used in the ML algorithm. Used as (timeDecay/SampleSize) in the ML algorithm
  * Default: 0.1
  * Range: 0 - 1.0

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
