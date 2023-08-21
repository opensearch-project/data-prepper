# Avro Sink/Output Codec

This is an implementation of Avro Sink Codec that parses the Data Prepper Events into Avro records and writes them into the underlying OutputStream.

## Usages

Avro Output Codec can be configured with sink plugins (e.g. S3 Sink) in the Pipeline file. 

## Configuration Options

```
pipeline:
  ...
  sink:
    - s3:
        aws:
          region: us-east-1
          sts_role_arn: arn:aws:iam::123456789012:role/Data-Prepper
          sts_header_overrides:
        max_retries: 5
        bucket: bucket_name
        object_key:
          path_prefix: vpc-flow-logs/%{yyyy}/%{MM}/%{dd}/
        threshold:
          event_count: 2000
          maximum_size: 50mb
          event_collect_timeout: 15s
        codec:
          avro:
            schema: >
              {
                "type" : "record",
                "namespace" : "org.opensearch.dataprepper.examples",
                "name" : "VpcFlowLog",
                "fields" : [
                  { "name" : "version", "type" : ["null", "string"]},
                  { "name" : "srcport", "type": ["null", "int"]},
                  { "name" : "dstport", "type": ["null", "int"]},
                  { "name" : "accountId", "type" : ["null", "string"]},
                  { "name" : "interfaceId", "type" : ["null", "string"]},
                  { "name" : "srcaddr", "type" : ["null", "string"]},
                  { "name" : "dstaddr", "type" : ["null", "string"]},
                  { "name" : "start", "type": ["null", "int"]},
                  { "name" : "end", "type": ["null", "int"]},
                  { "name" : "protocol", "type": ["null", "int"]},
                  { "name" : "packets", "type": ["null", "int"]},
                  { "name" : "bytes", "type": ["null", "int"]},
                  { "name" : "action", "type": ["null", "string"]},
                  { "name" : "logStatus", "type" : ["null", "string"]}
                ]
              }
            exclude_keys:
              - s3
        buffer_type: in_memory
```

## AWS Configuration

### Codec Configuration:

1) `schema`: A json string that user can provide in the yaml file itself. The codec parses schema object from this schema string. 
2) `exclude_keys`: Those keys of the events that the user wants to exclude while converting them to avro records.

### Note:

1) User can provide only one schema at a time i.e. through either of the ways provided in codec config.
2) If the user wants the tags to be a part of the resultant Avro Data and has given `tagsTargetKey` in the config file, the user also has to modify the schema to accommodate the tags. Another field has to be provided in the `schema.json` file:

    `{
   "name": "yourTagsTargetKey",
   "type": { "type": "array",
   "items": "string"
   }`
3) If the user doesn't provide any schema, the codec will auto-generate schema from the first event in the buffer.

## Developer Guide

This plugin is compatible with Java 11. See below

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:s3-sink:integrationTest -Dtests.s3sink.region=<your-aws-region> -Dtests.s3sink.bucket=<your-bucket>
```
