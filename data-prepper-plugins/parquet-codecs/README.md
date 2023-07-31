# Parquet Sink/Output Codec

This is an implementation of Parquet Sink Codec that parses the Dataprepper Events into Parquet Records and writes them into the underlying OutputStream.

## Usages

Parquet Output Codec can be configured with sink plugins (e.g. S3 Sink) in the Pipeline file.

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
          path_prefix: my-elb/%{yyyy}/%{MM}/%{dd}/
        threshold:
          event_count: 2000
          maximum_size: 50mb
          event_collect_timeout: 15s
       codec:
          parquet:
           tabular_schema: |
              TABLE Person (colname1 datatype,
                            colname2 datatype,
              			    colname3 datatype,
              			    colname4 datatype)
           schema: "{\"namespace\": \"org.example.test\"," +
                " \"type\": \"record\"," +
                " \"name\": \"TestMessage\"," +
                " \"fields\": [" +
                "     {\"name\": \"name\", \"type\": \"string\"}," +
                "     {\"name\": \"age\", \"type\": \"int\"}]" +
                "}";
            schema_file_location: "C:\\Path\\to\\your\\schema.json"
            schema_registry_url: https://your.schema.registry.url.com
            exclude_keys:
              - s3
            region: <yourAwsRegion>
            bucket: <yourBucket>
            path_prefix: <pathToFolder>
            buffer_type: in_memory
```

## AWS Configuration

### Codec Configuration:

1) `schema`: A json string that user can provide in the yaml file itself. The codec parses schema object from this schema string. 
2) `schema_file_location`: Path to the schema json file through which the user can provide schema.
3) `exclude_keys`: Those keys of the events that the user wants to exclude while converting them to avro records.
4) `schema_registry_url`: Another way of providing the schema through schema registry.
5) `region`: AWS Region of the S3 bucket which the user wants to use as buffer for records parsed by Parquet Output Codec.
6) `bucket`: Name of the S3 bucket which the user wants to use as buffer for records parsed by Parquet Output Codec.
7) `path_prefix`: Path to the folder within the S3 bucket where the user wants the intermittent files to be made.
8) `schema_bucket`: Name of the S3 bucket in which `schema.json` file is kept.
9) `file_key`: File key of `schema.json` file kept in S3 bucket.
10) `schema_region`: AWS Region of the S3 bucket in which `schema.json` file is kept.
11) `tabular_schema`: A multiline schema string like glue schema string that user can provide in the yaml file itself. The codec build schema object from this schema string.
### Note:

1) User can provide only one schema at a time i.e. through either of the ways provided in codec config.
2) If the user wants the tags to be a part of the resultant Avro Data and has given `tagsTargetKey` in the config file, the user also has to modify the schema to accommodate the tags. Another field has to be provided in the `schema.json` file:

    `{
   "name": "yourTagsTargetKey",
   "type": { "type": "array",
   "items": "string"
   }`

3) The user must provide valid `region`, `bucket` and `path_prefix` for the codec to work. 
4) If the user wants to input schema through a `schema.json` file kept in S3, the user must provide corresponding credentials i.e. region, bucket name and file key of the same.


## Developer Guide

This plugin is compatible with Java 11. See below

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

The integration tests for this plugin do not run as part of the Data Prepper build.

The following command runs the integration tests:

```
./gradlew :data-prepper-plugins:s3-sink:integrationTest -Dtests.s3sink.region=<your-aws-region> -Dtests.s3sink.bucket=<your-bucket>
```
