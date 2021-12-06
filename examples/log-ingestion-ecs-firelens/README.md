## Example: Sending Logs through ECS Firelens to hosted Data-Prepper with AWS Fluent Bit

[FireLens](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_firelens.html) is a container log router for Amazon ECS and AWS Fargate that gives you extensibility to use the breadth of services at AWS or partner solutions for log analytics and storage. FireLens works with Fluent Bit. This means you can use Fluent Bit's [HTTP output plugin](https://docs.fluentbit.io/manual/pipeline/outputs/http) to send your container's log to external or aws hosted data-prepper.

To route application logs from ECS container to [AWS for Fluent Bit](https://github.com/aws/aws-for-fluent-bit), specify `awsfirelens` as the `logDriver` in the `logConfiguration` object. Also, the key-value pairs specified as `options` in the `logConfiguration` object are used to generate the Fluent Bit output configuration to hosted Data-Prepper:

```
"logConfiguration": {
    "logDriver": "awsfirelens",
    "options": {
      "Format": "json",
      "Host": "<data-prepper-endpoint>",
      "Name": "http",
      "Port": "<data-prepper-http-source-port>",
      "URI": "/log/ingest"
    }
}
```

In this example [task definition](task-definition.json), an `nginx` ECS container task with family name `firelens-example-data-prepper` 
is created in a given ECS cluster with AWS Fluent Bit as log router to route the container logs to external Data-Prepper instance 
configured with the [example log pipeline YAML config](example_log_pipeline.yaml). One can send curl request to the `nginx` endpoint: 
```
curl <Public IP>
```
where `<Public IP>` is exposed by the ECS task `Networking` and observe the grokked records output from data-prepper `stdout` such as:

```
{"date":1.638805848186957E9,"container_name":"app","source":"stdout","log":"70.113.36.5 - - [06/Dec/2021:15:50:48 +0000] \"GET / HTTP/1.1\" 200 615 \"-\" \"curl/7.64.1\" 
\"-\"","container_id":"f4b6c7a71bbf49aea7b1c5c6a6bd9aa9-527074092","ecs_cluster":"firelens-dataprepper","ecs_task_arn":"arn:aws:ecs:us-east-1:531516510575:task/firelens-d
ataprepper/f4b6c7a71bbf49aea7b1c5c6a6bd9aa9","ecs_task_definition":"firelens-example-data-prepper:1","request":"/","auth":"-","ident":"-","response":"200","bytes":"615","
clientip":"70.113.36.5","verb":"GET","httpversion":"1.1","timestamp":"06/Dec/2021:15:50:48 +0000"}
```
### References:
* To create a task definition using ECS console, please refer to [Creating a task definition using the classic console](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/create-task-definition-classic.html) 
that allows configure task definition via JSON or [Creating a task definition using the new console](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/create-task-definition.html).

* For more details on ECS log routing, please refer to [Custom log routing](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_firelens.html).

* For more example task definitions demonstrating common custom log routing options, please see Amazon ECS documentation [Example task definitions](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/firelens-example-taskdefs.html) or 
[Amazon ECS FireLens examples on GitHub](https://github.com/aws-samples/amazon-ecs-firelens-examples).