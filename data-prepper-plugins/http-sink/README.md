# Http Sink

This is the Data Prepper Http sink plugin that sends records to http/https endpoints. You can use the sink to send data to arbitrary HTTP Endpoints.


## Usages

The Http sink should be configured as part of Data Prepper pipeline yaml file.

### Response status

* `200`: the request data has been successfully pushed to http endpoint.
* `500`: internal server error while process the request data.
* `400`: bad request error
* `404`: the http endpoint is not reachable
* `501`: the server does not recognize the request method and is incapable of supporting it for any resource

### HTTP Basic authentication
```
pipeline:
  ...
  sink:
  - http:
      authentication:
        http_basic:
          username: my-user
          password: my_s3cr3t
```

### HTTP Bearer token authentication
```
pipeline:
  ...
  sink:
  - http:
      authentication:
         bearer_token:
            client_id: 0oaafr4j79segrYGC5d7
            client_secret: fFel-3FutCXAOndezEsOVlght6D6DR4OIt7G5D1_oJ6w0wNoaYtgU17JdyXmGf0M
            token_url: https://localhost/oauth2/default/v1/token
            grant_type: client_credentials
            scope: httpSink
```

## Configuration

- `url` The http/https endpoint url.

- `proxy`(optional): A String of the address of a forward HTTP proxy. The format is like "<host-name-or-ip>:\<port\>". Examples: "example.com:8100", "http://example.com:8100", "112.112.112.112:8100". Note: port number cannot be omitted.

- `codec` : This plugin is integrated with sink codec

- `http_method` (Optional) : HttpMethod to be used. Default is POST.

- `auth_type` (Optional): Authentication type configuration. By default, this runs an unauthenticated server.

- `username`(optional): A string of username required for basic authentication

- `password`(optional): A string of password required for basic authentication

- `client_id`: It is the client id is the public identifier of your authorization server.

- `client_secret` : It is a secret known only to the application and the authorization server.

- `token_url`: The End point URL of the OAuth server.(Eg: /oauth2/default/v1/token)

- `grant_type` (Optional) : This grant type refers to the way an application gets an access token.

- `scope` (Optional) : This scope limit an application's access to a user's account.

- `aws_sigv4`: A boolean flag to sign the HTTP request with AWS credentials. Default to `false`.

- `aws` (Optional) : AWS configurations. See [AWS Configuration](#aws_configuration) for details. SigV4 is enabled by default when this option is used. If this option is present, `aws_` options are not expected to be present. If any of `aws_` options are present along with this, error is thrown.

- `custom_header` (Optional) : A Map<String, List<String> for custom headers such as AWS Sagemaker etc

- `dlq_file`(optional): A String of absolute file path for DLQ failed output records. Defaults to null.
  If not provided, failed records will be written into the default data-prepper log file (`logs/Data-Prepper.log`). If the `dlq` option is present along with this, an error is thrown.

- `dlq` (optional): DLQ configurations. See [DLQ](https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/failures-common/src/main/java/org/opensearch/dataprepper/plugins/dlq/README.md) for details. If the `dlq_file` option is present along with this, an error is thrown.

- `buffer_type`(optional) : Buffer type can be in_memory/local_file. Default is in_memory

- `max_retries`(optional): A number indicating the maximum number of times Http Sink should try to push the data to the Http arbitrary endpoint before considering it as failure. Defaults to `Integer.MAX_VALUE`.

### SSL

* ssl(Optional) => A `boolean` that enables mTLS/SSL. Default is ```false```.
* ssl_certificate_file(Optional) => A `String` that represents the SSL certificate chain file path or AWS S3 path. S3 path example `s3://<bucketName>/<path>`. Required if `ssl` is set to `true` and `use_acm_certificate_for_ssl` is set to `false`.
* ssl_key_file(Optional) => A `String` that represents the SSL key file path or AWS S3 path. S3 path example `s3://<bucketName>/<path>`. Only decrypted key file is supported. Required if `ssl` is set to `true` and `use_acm_certificate_for_ssl` is set to `false`.
* use_acm_certificate_for_ssl(Optional) : A `boolean` that enables mTLS/SSL using certificate and private key from AWS Certificate Manager (ACM). Default is `false`.
* acm_certificate_arn(Optional) : A `String` that represents the ACM certificate ARN. ACM certificate take preference over S3 or local file system certificate. Required if `use_acm_certificate_for_ssl` is set to `true`.
* acm_private_key_password(Optional): A `String` that represents the ACM private key password which that will be used to decrypt the private key. If it's not provided, a random password will be generated.
* acm_certificate_timeout_millis(Optional) : An `int` that represents the timeout in milliseconds for ACM to get certificates. Default value is `120000`.

### <a name="aws_configuration">AWS Configuration</a>

* `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).
* `sts_role_arn` (Optional) : The STS role to assume for requests to AWS. Defaults to null, which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html).
* `sts_header_overrides` (Optional): A map of header overrides to make when assuming the IAM role for the sink plugin.
* `sts_external_id` (Optional): An optional external ID to use when assuming an IAM role.

### Threshold
* event_count => The event_count size should be between 0 and 10000000
* maximum_size => The size of byte capacity, Default is 50mb
* event_collect_timeout => The event_collect timeout is between 1 and 3600 seconds

## Metrics

### Counter

- `httpSinkRecordsSuccessCounter`: measures total number of records successfully pushed to http end points (200 response status code) by HTTP sink plugin.
- `httpSinkRecordsFailedCounter`: measures total number of records failed to pushed to http end points (500/400/404/501 response status code) by HTTP sink plugin.

### End-to-End acknowledgements

If the events received by the Http Sink have end-to-end acknowledgements enabled (which is tracked using the presence of EventHandle in the event received for processing), then upon successful posting to OpenSearch or upon successful write to DLQ, a positive acknowledgement is sent to the acknowledgementSetManager, otherwise a negative acknowledgement is sent.

## Developer Guide

This plugin is compatible with Java 8. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
