# OTel Trace Group Processor

This is a processor that fills in the missing trace group related fields in the collection of [Span](../../data-prepper-api/src/main/java/org/opensearch/dataprepper/model/trace/Span.java) records by looking up the opensearch backend.
It finds the missing trace group info for a spanId by looking up the relevant fields in its root span stored in OpenSearch or Amazon OpenSearch Service backend that the local data-prepper host ingest into.

## Usages

### OpenSearch

```
pipeline:
  ...
  processor:
    - otel_trace_group:
        hosts: ["https://localhost:9200"]
        cert: path/to/cert
        username: YOUR_USERNAME_HERE
        password: YOUR_PASSWORD_HERE
```

See [opensearch_security.md](../opensearch/opensearch_security.md) for detailed explanation.

### Amazon OpenSearch Service

```
pipeline:
  ...
  processor:
    - otel_trace_group:
        hosts: ["https://your-amazon-opensearch-service-endpoint"]
        aws_sigv4: true
        cert: path/to/cert
        insecure: false
```

See [security.md](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-plugins/opensearch/security.md) for detailed explanation.

## Configuration

- `hosts`: A list of IP addresses of OpenSearch nodes.

- `cert`(optional): CA certificate that is pem encoded. Accepts both .pem or .crt. This enables the client to trust the CA that has signed the certificate that OpenSearch is using.
Default is null.

- `aws_sigv4`: A boolean flag to sign the HTTP request with AWS credentials. Only applies to Amazon OpenSearch Service. See [security](security.md) for details. Default to `false`.

- `aws_region`: A String represents the region of Amazon OpenSearch Service domain, e.g. us-west-2. Only applies to Amazon OpenSearch Service. Defaults to `us-east-1`.

- `aws_sts_role_arn`: A IAM role arn which the sink plugin will assume to sign request to Amazon OpenSearch Service. If not provided the plugin will use the default credentials.

- `aws_sts_external_id`: The external ID to attach to all AssumeRole requests.

- `aws_sts_header_overrides`: An optional map of header overrides to make when assuming the IAM role for the sink plugin.

- `insecure`: A boolean flag to turn off SSL certificate verification. If set to true, CA certificate verification will be turned off and insecure HTTP requests will be sent. Default to `false`.

- `username`(optional): A String of username used in the [internal users](https://opensearch.org/docs/latest/security-plugin/access-control/users-roles) of OpenSearch cluster. Default is null.

- `password`(optional): A String of password used in the [internal users](https://opensearch.org/docs/latest/security-plugin/access-control/users-roles) of OpenSearch cluster. Default is null.

## Metrics

### Counter
- `recordsInMissingTraceGroup`: number of ingress records missing trace group fields.
- `recordsOutFixedTraceGroup`: number of egress records with trace group fields filled successfully.
- `recordsOutMissingTraceGroup`: number of egress records missing trace group fields.

## Developer Guide

This plugin is compatible with Java 8. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
