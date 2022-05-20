# Data Prepper Monitoring in Embedded Metrics Format (EMF)
This directory contains the example configuration files for allowing exporting Data Prepper runtime metrics in [Embedded Metrics Format](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Specification.html) logs into Fluent Bit agent/sidecar to facilitate stream processing in the downstream services. Currently it includes two use cases:
(1) vanilla Fluent Bit; (2) [ECS Firelens](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_firelens.html) as managed version of Fluent Bit. Both depends on the following `meterRegistry` configuration in `data-prepper-config.yaml`

```
metricRegistries:
  - "EmbeddedMetricsFormat"
```

Data Prepper uses [aws-embedded-metrics-java](https://github.com/awslabs/aws-embedded-metrics-java) sdk to publish its EMF metric logs over TCP to Fluent Bit sidecar which then delivers to third-party streaming service (e.g. Kinesis Data Streams, Apache Kafka).

## Fluent Bit

A demo Data Prepper monitoring setup can be launched under the current directory by

```
docker-compose up -d
```

which involves the following files:
* [docker-compose.yml](./docker-compose.yml): running Data Prepper container built from the repo with Fluent Bit sidecar
* [Dockerfile](./Dockerfile): Dockerfile for building Data Prepper image out of the current repo.
* [fluent-bit.conf](./fluent-bit.conf): FluentBit configuration file that sets up the tcp source listener. It uses standard output for demo purpose.
* [data-prepper-config.yaml](./data-prepper-config.yaml): Data Prepper service configuration file. Note that EmbeddedMetricsFormat meter registry is specified.
* [pipelines-raw-trace-stdout.yaml](./pipelines-raw-trace-stdout.yaml): Data Prepper demo pipeline definition.

The TCP connection between [EMF metrics log publisher](https://github.com/awslabs/aws-embedded-metrics-java#configuration) in Data Prepper and the Fluent Bit source is established through the environment variable `AWS_EMF_AGENT_ENDPOINT`. To confirm runtime metrics collected from Data Prepper, one can check the `stdout` from Fluent Bit container logs as follows:

```
[0] emf-test: [1652757960.078408100, {"log"=>"{"_aws":{"Timestamp":1652757960004,"CloudWatchMetrics":[{"Namespace":"DataPrepper","Metrics":[{"Name":"jvm.buffer.memory.used.value","Unit":"Bytes"}],"Dimensions":[["id","serviceName"]]}],"LogGroupName":"Unknown-metrics"},"serviceName":"dataprepper","jvm.buffer.memory.used.value":0.0,"id":"mapped"}"}]
[1] emf-test: [1652757960.078611700, {"log"=>"{"_aws":{"Timestamp":1652757960004,"CloudWatchMetrics":[{"Namespace":"DataPrepper","Metrics":[{"Name":"armeria.executor.queue.remaining.value","Unit":"None"}],"Dimensions":[["name","serviceName"]]}],"LogGroupName":"Unknown-metrics"},"name":"blockingTaskExecutor","serviceName":"dataprepper","armeria.executor.queue.remaining.value":2.147483647E9}"}]
[2] emf-test: [1652757960.078887300, {"log"=>"{"_aws":{"Timestamp":1652757960004,"CloudWatchMetrics":[{"Namespace":"DataPrepper","Metrics":[{"Name":"jvm.threads.states.value","Unit":"None"}],"Dimensions":[["serviceName","state"]]}],"LogGroupName":"Unknown-metrics"},"serviceName":"dataprepper","jvm.threads.states.value":0.0,"state":"terminated"}"}]
[3] emf-test: [1652757960.079018700, {"log"=>"{"_aws":{"Timestamp":1652757960004,"CloudWatchMetrics":[{"Namespace":"DataPrepper","Metrics":[{"Name":"jvm.memory.committed.value","Unit":"Bytes"}],"Dimensions":[["area","id","serviceName"]]}],"LogGroupName":"Unknown-metrics"},"area":"nonheap","jvm.memory.committed.value":5.3477376E7,"id":"Metaspace","serviceName":"dataprepper"}"}]
[4] emf-test: [1652757960.079167300, {"log"=>"{"_aws":{"Timestamp":1652757960004,"CloudWatchMetrics":[{"Namespace":"DataPrepper","Metrics":[{"Name":"armeria.executor.scheduled.once.count","Unit":"Count"}],"Dimensions":[["name","serviceName"]]}],"LogGroupName":"Unknown-metrics"},"name":"blockingTaskExecutor","serviceName":"dataprepper","armeria.executor.scheduled.once.count":0.0}"}]
[5] emf-test: [1652757960.079317200, {"log"=>"{"_aws":{"Timestamp":1652757960004,"CloudWatchMetrics":[{"Namespace":"DataPrepper","Metrics":[{"Name":"jvm.memory.used.value","Unit":"Bytes"}],"Dimensions":[["area","id","serviceName"]]}],"LogGroupName":"Unknown-metrics"},"area":"nonheap","id":"Compressed Class Space","serviceName":"dataprepper","jvm.memory.used.value":7369400.0}"}]
[6] emf-test: [1652757960.079396600, {"log"=>"{"_aws":{"Timestamp":1652757960004,"CloudWatchMetrics":[{"Namespace":"DataPrepper","Metrics":[{"Name":"process.cpu.usage.value","Unit":"None"}],"Dimensions":[["serviceName"]]}],"LogGroupName":"Unknown-metrics"},"serviceName":"dataprepper","process.cpu.usage.value":0.0}"}]
[7] emf-test: [1652757960.079438400, {"log"=>"{"_aws":{"Timestamp":1652757960004,"CloudWatchMetrics":[{"Namespace":"DataPrepper","Metrics":[{"Name":"raw-pipeline.otel_trace_raw_prepper.timeElapsed.sum","Unit":"Milliseconds"},{"Name":"raw-pipeline.otel_trace_raw_prepper.timeElapsed.count","Unit":"Count"}],"Dimensions":[["serviceName"]]}],"LogGroupName":"Unknown-metrics"},"serviceName":"dataprepper","raw-pipeline.otel_trace_raw_prepper.timeElapsed.sum":0.0,"raw-pipeline.otel_trace_raw_prepper.timeElapsed.count":0.0}"}]
```

## ECS Firelens

ECS Firelens as a custom log router allows user to specify a custom Fluent Bit source by [baking the *.conf into the docker image](https://github.com/aws-samples/amazon-ecs-firelens-examples/tree/mainline/examples/fluent-bit/config-file-type-file). This demo uses cloudwatch as output for ECS Firelens. 
It involves the following files:
* [firelens-fluent-bit.conf](./firelens-fluent-bit.conf): counterpart of [fluent-bit.conf](./fluent-bit.conf) with output replaced by cloudwatch logs.
* [firelens.Dockerfile](./firelens.Dockerfile): Dockerfile for building a custom Fluent Bit image with [firelens-fluent-bit.conf](./firelens-fluent-bit.conf) added into its filepath.
* [data-prepper-emf-demo-cfn.yaml](./data-prepper-emf-demo-cfn.yaml): A CloudFormation template to deploy a ECS Fargate task definition with Data Prepper and ECS Firelens.

### Steps

1. Build Data Prepper image according to release process. e.g.

Under [release](../../../release) directory
```
./gradlew :release:docker:docker -Prelease
docker tag opensearch-data-prepper:1.4.0-SNAPSHOT <account-id>.dkr.ecr.<region>.amazonaws.com/data-prepper-poc:EMF-METRICS-LOGGING-POC
```

2. Push the built image to Elastic Container Registry (ECR) repository. e.g.
```
aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <account-id>.dkr.ecr.<region>.amazonaws.com
docker push <account-id>.dkr.ecr.<region>.amazonaws.com/data-prepper-poc:EMF-METRICS-LOGGING-POC
```
3. Build custom Fluent Bit image and push to ECR repository:
```
docker build -f firelens.Dockerfile  -t 531516510575.dkr.ecr.us-west-2.amazonaws.com/aws-for-fluent-bit:DEMO .
docker push 531516510575.dkr.ecr.us-west-2.amazonaws.com/aws-for-fluent-bit:DEMO
```
4. Deploy CloudFormation template that creates ECS Fargate task definition
```
aws cloudformation deploy --template-file data-prepper-emf-demo-cfn.yaml --stack-name data-prepper-emf-demo-task
```
Notice that one needs to grant [Cloudwatch permissions](https://github.com/aws-samples/amazon-ecs-firelens-examples/blob/mainline/examples/fluent-bit/cloudwatchlogs/permissions.json) for `TaskRoleArn` specified in the template.
5. Launch ECS cluster and create service with the task definition created above. For details, refer to [ECS documentation](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/Welcome.html)
6. One can verify the generated cloudwatch log group and log stream in the AWS console with log events as follows:
```
{
    "_aws": {
        "Timestamp": 1652817420001,
        "CloudWatchMetrics": [
            {
                "Namespace": "DataPrepper",
                "Metrics": [
                    {
                        "Name": "armeria.server.connections.lifespan.percentile.value",
                        "Unit": "Milliseconds"
                    }
                ],
                "Dimensions": [
                    [
                        "phi",
                        "protocol"
                    ]
                ]
            }
        ]
    },
    "phi": "0.9",
    "createdAt": "2022-05-12T21:40:50.251064831Z",
    "image": "531516510575.dkr.ecr.us-west-2.amazonaws.com/data-prepper-poc:EMF-METRICS-LOGGING-POC",
    "cluster": "arn:aws:ecs:us-west-2:531516510575:cluster/Data-Prepper-Monitoring",
    "protocol": "h1c",
    "taskArn": "arn:aws:ecs:us-west-2:531516510575:task/Data-Prepper-Monitoring/7d02540db05e4c17bf564f1de0a1dceb",
    "startedAt": "2022-05-12T21:40:50.251064831Z",
    "containerId": "ip-172-31-15-237.us-west-2.compute.internal",
    "armeria.server.connections.lifespan.percentile.value": 0.237568
}
```