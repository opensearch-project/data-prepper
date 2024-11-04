# AWS Lambda Processor and Sink for Data Prepper

This document provides the configuration details and usage instructions for integrating AWS Lambda with Data Prepper, both as a processor and as a sink.

----------------------------------------------------------------------------------------
## AWS Lambda Processor
Configuration
The aws_lambda processor allows you to invoke an AWS Lambda function in your Data Prepper pipeline to process events. This can be used for synchronous or asynchronous invocations based on your requirements.

Configuration Fields:

```
Field                | Type    | Required | Description                                                                 
-------------------- | ------- | -------- | ---------------------------------------------------------------------------- 
function_name        | String  | Yes      | The name of the AWS Lambda function to invoke.                               
invocation_type      | String  | Yes      | Specifies the invocation type: either request-response or event. Default is request-response           
aws.region           | String  | Yes      | The AWS region where the Lambda function is located.                         
aws.sts_role_arn     | String  | No       | ARN of the role to assume before invoking the Lambda function.               
max_retries          | Integer | No       | Maximum number of retries if the invocation fails. Default is 3.             
batch                | Object  | No       | Batch settings for the Lambda invocations. Default key_name = "events". Default Threshold for event_count=100, maximum_size="5mb", event_collect_timeout = 10s                            
lambda_when          | String  | No       | Conditional expression to determine when to invoke the Lambda processor.     
response_codec       | Object  | No       | Codec configuration for parsing Lambda responses. Default is json
tags_on_match_failure| List    | No       | A List of Strings that specifies the tags to be set in the event when lambda fails to match or an unknown exception occurs while matching.
sdk_timeout          | Duration| No       | Defines the time, sdk maintains the connection to the client before timing out. Default is 60s 
response_events_match| boolean | No       | Defines the way Data Prepper treats the response from Lambda. Default is false
```

Example Configuration:
```
processors:
  - aws_lambda:
      function_name: "my-lambda-function"
      invocation_type: "request-response"
      response_events_match: false
      aws:
        region: "us-east-1"
        sts_role_arn: "arn:aws:iam::123456789012:role/my-lambda-role"
      max_retries: 3
      batch:
        key_name: "events"
        threshold:
          event_count: 100
          maximum_size: "5mb"
          event_collect_timeout: PT10S
      lambda_when: "event['status'] == 'process'"

```

## Usage
Invocation Type:
- request-response: Waits for the Lambda function's response before continuing.
- event: Invokes the function asynchronously without waiting for a response.
  Batching: If batching is enabled by default, events are grouped together and sent in bulk to reduce Lambda invocations. The threshold within batch defines the number of events, size limit, or timeout for batching.
  Codec: Currently both request and response codecs are json. Processor response requires lambda to send back a `Json Array` only.
  tags_on_match_failure: A List of Strings that specifies the tags to be set in the event when lambda fails to match or an unknown exception occurs while matching. This tag may be used in conditional expressions in other parts of the configuration

## Behaviour
When the AWS Lambda processor in Data Prepper is configured for batching, it groups multiple events together into a single request based on the batch thresholds (event count, size, or time). The entire batch is sent to the Lambda function as a single payload.

Lambda Response Handling:
response_events_match configuration defines how the relationship of each events in a batch as a part of request to lambda and the response from lambda.
- True: Lambda typically returns a JSON array containing the results for each event in the batch. Data Prepper will map this array back to the individual events, ensuring that each event in the batch gets the corresponding part of the response from the array.
- False: Lambda could return one or multiple events back in the response for all events in a batch. but they will not be corelated back to the original events.
  Here correlation means that that the original events metadata etc will be carry forwarded to the response events.
  If response_events_match is set to true, the expectation are:
1) User should return same number of response events as requests
2) Order should be maintained


## Limitations
- payload limitation: 6mb payload limit
- response codec - supports only json codec


## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.
The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:aws-lambda:integrationTest -Dtests.processor.lambda.region="us-east-1" -Dtests.processor.lambda.functionName="lambda_test_function"  -Dtests.processor.lambda.sts_role_arn="arn:aws:iam::123456789012:role/dataprepper-role

```

----------------------------------------------------------------------------------------

## AWS Lambda Sink

```
Field             | Type    | Required | Description                                                                 
----------------- | ------- | -------- | ---------------------------------------------------------------------------- 
function_name     | String  | Yes      | The name of the AWS Lambda function to invoke.                               
invocation_type   | String  | No       | Specifies the invocation type. Default is event.             
aws.region        | String  | Yes      | The AWS region where the Lambda function is located.                         
aws.sts_role_arn  | String  | No       | ARN of the role to assume before invoking the Lambda function.               
max_retries       | Integer | No       | Maximum number of retries if the invocation fails. Default is 3.             
batch             | Object  | No       | Optional batch settings for Lambda invocations. Default key_name = "events". Default Threshold for event_count=100, maximum_size="5mb", event_collect_timeout = 10s                              
lambda_when       | String  | No       | Conditional expression to determine when to invoke the Lambda sink.          
dlq               | Object  | No       | Dead-letter queue (DLQ) configuration for failed invocations.                
```

Example Configuration:
```
sink:
  - aws_lambda:
      function_name: "my-lambda-sink"
      invocation_type: "event"
      aws:
        region: "us-west-2"
        sts_role_arn: "arn:aws:iam::123456789012:role/my-lambda-sink-role"
      max_retries: 5
      batch:
        key_name: "events"
        threshold:
          event_count: 50
          maximum_size: "3mb"
          event_collect_timeout: PT5S
      lambda_when: "event['type'] == 'log'"
      dlq:
        region: "us-east-1"
        sts_role_arn: "arn:aws:iam::123456789012:role/my-sqs-role"
        bucket: "<<your-dlq-bucket-name>>"
```

Usage
Invocation Type:
- event: Invokes the function asynchronously without waiting for a response.
- request-response: Not supported in sink
Batching: Batching is enabled by default, events are grouped together based on the defined threshold in the batch configuration.
Dead-Letter Queue (DLQ): A DLQ can be configured to handle failures in Lambda invocations. If the invocation fails after retries, the failed events will be sent to the specified DLQ


## Additional Notes
IAM Role Assumption: Both the processor and sink can assume a specified IAM role (aws.sts_role_arn) before invoking Lambda functions. This allows for more secure handling of AWS resources.
Concurrency Considerations: When using the event invocation type, be mindful of Lambda concurrency limits to avoid throttling.
For further details on AWS Lambda integration with Data Prepper, refer to the AWS Lambda documentation: https://docs.aws.amazon.com/lambda

## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.
The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:aws-lambda:integrationTest -Dtests.sink.lambda.region="us-east-1" -Dtests.sink.lambda.functionName="lambda_test_function"  -Dtests.sink.lambda.sts_role_arn="arn:aws:iam::123456789012:role/dataprepper-role

```
