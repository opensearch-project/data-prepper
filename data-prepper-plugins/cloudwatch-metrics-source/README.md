# CloudWatch Metrics Source

This is the Data Prepper Cloudwatch Metrics Source plugin which will fetch the metrics based on user configuration and writes to buffer.

## Usages

The Cloudwatch Metrics Source should be configured as part of Data Prepper pipeline yaml file.

## Configuration Options

```
cloudwatch-metrics-source-pipeline:
  source:
    cloudwatch:
      aws:
        region: us-east-1
        sts_role_arn: arn:aws:iam::123456789012:role/Data-Prepper
      batch_size: 1000
      namespaces:
        - namespace:
            name: "AWS/S3"
            start_time: 
            end_time: 
            metricDataQueries:
              - metric:
                  name:                   
                  id: "q1"
                  period: 
                  stat: "Average"
                  unit: "Bytes"
                  dimensions:
                    - dimension:
                        name: 
                        value: 
                    - dimension:
                        name: 
                        value: 
              - metric:
                  name: 
                  id: "q2"
                  period: 
                  stat: "Average"
                  dimensions:
                    - dimension:
                        name: 
                        value: 
                    - dimension:
                        name: 
                        value: 
        - namespace:
            name: "AWS/EC2"
            start_time: 
            end_time: 
            metricDataQueries:
              - metric:
                  name: 
                  id: "q1"
                  period: 
                  stat: "Average"
                  dimensions:
                    - dimension:
                        name: 
                        value: 
  sink:
    - stdout:
```

## AWS Configuration

- `region` (Required) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).

- `sts_role_arn` (Required) : The AWS STS role to assume for requests to CloudWatch. which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html).

- `batch_size` (Required) : The batch size used for flushing record events to the buffer.

## namespaces Configuration

- `namespace` (Required) : List used to specify the AWS namespace configuration.

    - `name` (Required) : Used to specify the AWS service for which the metrics are scraped. Example namespace is "AWS/EC2".

    - `start_time` (Required) : Start time to scrape the metrics. The format should be UTC as follows: "yyyy-mm-ddTHH:MM:SSz". Example start_time is "2023-07-19T18:35:24z"

    - `end_time` (Required) : End time to scrape the metrics. The format should be UTC as follows: "yyyy-mm-ddTHH:MM:SSz". Example end_time is "2023-07-20T18:35:24z"
  
       ### metricDataQueries Configuration
  
    - `metric_name` (Required) : Metric name that user wish to scrape for the mentioned namespace. Example metrics for EC2 are "CPUUtilization", "DiskReadOps" ...etc
  
    - `id` (Required) : id of the metrics

    - `period` (Required) : time period
  
    - `stat` (Required) : stat value
 
       ### dimensions Configuration
  
    - `name` (Optional) : name of the dimension

    - `value` (Optional) : value of the dimension
      

## Developer Guide

This plugin is compatible with Java 11. See below

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:cloudwatch-metrics-source:integrationTest -Dtests.cloudwatch-metric-scource.region=<your-aws-region>
```
