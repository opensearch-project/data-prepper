### Example: Sending Logs through ECS Firelens to hosted Data-Prepper with AWS Fluent Bit

[FireLens](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_firelens.html) is a container log router for Amazon ECS and AWS Fargate that gives you extensibility to use the breadth of services at AWS or partner solutions for log analytics and storage. FireLens works with Fluent Bit. This means one can use Fluent Bit's [HTTP output plugin](https://docs.fluentbit.io/manual/pipeline/outputs/http) to send your container's log to external or aws hosted data-prepper.

To route application logs from ECS container to [AWS for Fluent Bit](https://github.com/aws/aws-for-fluent-bit), one needs to specify `awsfirelens` as the `logDriver` in the logConfiguration object. Also, the key-value pairs specified as `options` in the logConfiguration object are used to generate the Fluent Bit output configuration. One can refer to this example [task definition](task-definition.json):

```
"logConfiguration": {
    "logDriver": "awsfirelens",
    "options": {
      "Format": "json",
      "Host": "<data-prepper-endpoint>",
      "Name": "http",
      "Port": "2021",
      "URI": "/log/ingest"
    }
}
```
and the [example log pipeline YAML config](example_log_pipeline.yaml) accordingly.

To create a task definition using the classic ECS console, please refer [here](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/create-task-definition-classic.html). 

For more details on ECS log routing, one could refer to the documentation [here](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_firelens.html).

For more example task definitions demonstrating common custom log routing options, please see Amazon ECS documentation [here](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/firelens-example-taskdefs.html) or 
[Amazon ECS FireLens examples on GitHub](https://github.com/aws-samples/amazon-ecs-firelens-examples).