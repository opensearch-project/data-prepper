
# ML Merge Processor

This plugin enables you to merge data from a S3 file with source data from your Data Prepper pipeline.

## Usage
```aidl
ml_merge-pipeline:
...
  processor:
    - ml_merge:
          fields:
            - to_key: field_B
              from_key: /modelInput/inputText
          merge_all_fields: true              # Whether to merge all fields from input source
          retry_on_failure: 3                 # Retry attempts for failed requests
          tags_on_failure: ["lookup_failed"]  # Tags for failed events
          # S3 source configuration
          source_path_prefix: "s3://bucket/input/"  # Base path for source/input files
          result_file_suffix: ".out"               # Suffix to identify result files
          correlation_field: "recordId"            # Field used to match related records
```
`fields` as the fields to be merged into the pipeline.
`merge_all_fields` as the flag to merge all data into the pipeline.
`source_path_prefix` as the Base path for source/input files
`correlation_field` as the field name used to match related records

# Metrics

### Counter

## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.
