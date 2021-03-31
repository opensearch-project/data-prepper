# OTel Trace Group Prepper

This is a prepper that fills in the missing trace group related fields in the collection of raw span string records output by [otel-trace-raw-prepper](../dataPrepper-plugins/otel-trace-raw-prepper) and then convert them back into a new collection of string records.
It finds the missing trace group info for a spanId by looking up the relevant fields in its root span stored in opendistro-for-elasticsearch (ODFE) or Amazon Elasticsearch Service backend that the local data-prepper host ingest into.

## Usages

### Opendistro-for-elasticsearch

```
pipeline:
  ...
  prepper:
    - otel-trace-group-prepper:
        hosts: ["https://localhost:9200"]
        cert: path/to/cert
        username: YOUR_USERNAME_HERE
        password: YOUR_PASSWORD_HERE
``` 

### Amazon Elasticsearch Service

```
pipeline:
  ...
  prepper:
    - otel-trace-group-prepper:
        hosts: ["https://your-amazon-elasticssearch-service-endpoint"]
        aws_sigv4: true 
        cert: path/to/cert
        insecure: false
```

## Configuration

- `hosts`: A list of IP addresses of elasticsearch nodes.

- `cert`(optional): CA certificate that is pem encoded. Accepts both .pem or .crt. This enables the client to trust the CA that has signed the certificate that ODFE is using.
Default is null. 

- `aws_sigv4`: A boolean flag to sign the HTTP request with AWS credentials. Only applies to Amazon Elasticsearch Service. See [security](security.md) for details. Default to `false`. 

- `aws_region`: A String represents the region of Amazon Elasticsearch Service domain, e.g. us-west-2. Only applies to Amazon Elasticsearch Service. Defaults to `us-east-1`.

- `insecure`: A boolean flag to turn off SSL certificate verification. If set to true, CA certificate verification will be turned off and insecure HTTP requests will be sent. Default to `false`.

- `username`(optional): A String of username used in the [internal users](https://opendistro.github.io/for-elasticsearch-docs/docs/security/access-control/users-roles) of ODFE cluster. Default is null.

- `password`(optional): A String of password used in the [internal users](https://opendistro.github.io/for-elasticsearch-docs/docs/security/access-control/users-roles) of ODFE cluster. Default is null.

## Metrics

TBD

## Developer Guide

This plugin is compatible with Java 8. See 

- [CONTRIBUTING](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/docs/readme/monitoring.md)