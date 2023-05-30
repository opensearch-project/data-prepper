# Anomaly Detector Processor

This is a processor that takes structured data and runs anomaly detection algorithm on user configured fields in the data. The data needs to be a number(integer or real) for the anomaly detection algorithm to detect anomalies. It is recommended that the anomaly detector processor is deployed after aggregate processor in a pipeline for best results because aggregate processor aggregates events with same keys onto the same host.

## Basic Usage

To get started, create the following `pipeline.yaml`. This following pipeline configuration will look for anomalies in the `latency` field in the events passed to the processor. It uses `random_cut_forest` mode to detect anomalies.

```yaml
ad-pipeline:
  source:
    http:
  processor:
    - anomaly_detector:
        keys: ["latency"]
        mode: 
            random_cut_forest:
  sink:
    - stdout:
```

When run, the processor will parse the messages and extracts the values for the key `latency` and pass it through RandomCutForest ML algorithm.

## Configuration

### Options

* [keys](#keys) (Required)
* [mode](#mode) (Required)

### <a name="keys"></a>
* `keys` (Required): A non-ordered `List<String>` which are used as inputs to the ML algorithm to detect anomalies in the values of the keys in the list. At least one key is required.

### <a name="mode"></a>
* `mode` (Required): The ML algorithm (or model) to use to detect anomalies. One of the existing [Modes](#anomaly-detector-modes) must be provided.
    * [random_cut_forest](#random_cut_forest)


## Available Anomaly detector modes

### <a name="random_cut_forest"></a>
* `random_cut_forest`: Processes events using Random Cut Forest ML algorithm to detect anomalies.
  * After passing a bunch of events with `latency` value between 0.2 and 0.3 are passed through the anomaly detector, when an event with `latency` value 11.5 is sent, the following anomaly event will be generated
  * More details about this can be found at https://docs.aws.amazon.com/sagemaker/latest/dg/randomcutforest.html
        ```json
            { "latency": 11.5, "deviation_from_expected":[10.469302736820003],"grade":1.0}
        ```
        Where `deviation_from_expected` is a list of deviations for each of the keys from their corresponding expected values and `grade` is the anomaly grade indicating the severity of the anomaly

#### Options
* `shingle_size` - shingle size to be used in the ML algorithm
  * Default: `4`
  * Range: 1 - 60
* `sample_size` - sample size size to be used in the ML algorithm
  * Default: `256`
  * Range: 100 - 2500
* `output_after` - number of events used to train the ML algorithm. Anomaliesare detected after seeing the configured number number of events and used fortraining. Value of this should be less than or equal to `sample_size`
  * Default: `256`
* `time_decay` - time decay value to be used in the ML algorithm. Used as (timeDecay/SampleSize) in the ML algorithm
  * Default: `0.1`
  * Range: 0 - 1.0
* `type` - Type of data that is being sent to the algorithm
  * Default: `metrics`
  * Others types like `traces` will be supported in future
* `version` - version of the algorithm
  * Default: `1.0`


## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
