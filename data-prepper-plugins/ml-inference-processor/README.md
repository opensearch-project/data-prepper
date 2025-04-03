
# ML Inference Processor

This plugin enables you to send data from your Data Prepper pipeline directly to ml-commons for machine learning related activities.

## Usage
```aidl
ml-inference-pipeline:
...
  processor:
    - ml_inference:
        host: "<your OpenSearch url>"
        aws_sigv4: true
        action_type: "batch_predict"
        service_name: "bedrock"
        model_id: "<your model id used in the ml-commons for vector search>"
        output_path: "<your batch job output in S3>"
        input_key: key
        aws:
          region: "us-east-1"
          sts_role_arn: "<arn>"
        ml_when: /bucket == "offlinebatch"

```
`model_id` as the model id that is registered in the OpenSearch ml-commons plugin.
`service_name` as the remote AI service platform to process then batch job.
`output_path` as the batch job output location of the S3 Uri

# Metrics

### Counter
- `BatchJobRequestsSucceeded`: measures total number of requests received and processed successfully by ml-inference-processor.
- `BatchJobRequestsFailed`: measures total number of requests failed by ml-inference-processor.
- `batchJobsCreationSucceeded`: measures total number of batch jobs successfully created (200 response status code) by OpenSearch ml-commons API.
- `batchJobsCreationFailed`: measures total number of batch jobs creation failed by OpenSearch ml-commons API.

## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.
