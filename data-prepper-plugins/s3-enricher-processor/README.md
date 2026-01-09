
# S3 Enricher Processor

This plugin enables you to merge data from a S3 file with source data from your Data Prepper pipeline.

## Usage
```aidl
ml_merge-pipeline:
...
  processor:
    - s3_enricher:
        # =============================================================================
        # S3 SOURCE BUCKET CONFIGURATION
        # Defines where to fetch the original/source data for enrichment
        # =============================================================================
        bucket:
          # The S3 bucket containing the source records to enrich from
          name: offlinebatch
          filter:
            # S3 prefix path where source files are located
            # The processor will look for source files under this prefix
            # Example: s3://offlinebatch/bedrockbatch/originsource/test_batch_50k.jsonl
            include_prefix: bedrockbatch/originsource/
    
        # =============================================================================
        # DATA FORMAT CONFIGURATION
        # =============================================================================
        # Codec for parsing source S3 files
        # Options: ndjson, json, csv, etc.
        codec:
          ndjson:
    
        # =============================================================================
        # AWS CONFIGURATION
        # =============================================================================
        # AWS account ID that owns the S3 bucket (for cross-account access)
        default_bucket_owner: 802041417063
    
        aws:
          # AWS region where the S3 bucket is located
          region: us-east-1
    
        # =============================================================================
        # S3 OBJECT SETTINGS
        # =============================================================================
        # Maximum size (in MB) of S3 source files to process
        # Files exceeding this limit will be skipped
        s3_object_size_limit_mb: 100
    
        # JSON path in the incoming pipeline event that contains the S3 object key
        # Used to determine which source file to fetch for enrichment
        # Example event: {"s3": {"bucket": "...", "key": "output/file.jsonl.out"}}
        s3_key_path: "s3/key"
    
        # =============================================================================
        # SOURCE FILE NAME EXTRACTION
        # =============================================================================
        # Regex pattern to extract the base filename from the output S3 key
        # The first capture group (.*?) extracts the original source filename
        #
        # Example:
        #   Input:  test_batch_50k-2025-11-06T21-19-15Z-1762463955825635000-uuid.jsonl.out
        #   Match:  Group 1 = "test_batch_50k"
        #   Result: Looks for source file "test_batch_50k.jsonl" in include_prefix path
        #
        # Pattern breakdown:
        #   ^(.*?)                    - Capture base filename (non-greedy)
        #   -\d{4}-\d{2}-\d{2}        - Match date: -YYYY-MM-DD
        #   T\d{2}-\d{2}-\d{2}Z       - Match time: THH-MM-SSZ
        #   -.*                       - Match remaining (job ID, UUID, etc.)
        #   \.jsonl\.out$             - Match file extension
        s3_object_name_pattern: ^(.*?)-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z-.*\.jsonl\.out$
    
        # =============================================================================
        # RECORD MATCHING & ENRICHMENT
        # =============================================================================
        # Field name used to correlate/match records between output and source files
        # Both the pipeline event and source records must contain this field
        # Records with matching correlation values will be merged
        correlation_key: "recordId"
    
        # List of fields to copy from the source record into the pipeline event
        # Only these specified fields will be merged; all other source fields are ignored
        # If a field doesn't exist in source, it will be skipped
        keys_to_merge:
          - "field_A"
          - "field_B"
          - "field_C"
    
        # =============================================================================
        # CONDITIONAL PROCESSING
        # =============================================================================
        # Data Prepper expression to conditionally apply enrichment
        # Only events matching this condition will be processed by the enricher
        # Events not matching will pass through unchanged
        enrich_when: /s3/key != null
```
`keys_to_merge` List of fields to copy from the source record into the pipeline event.
`s3_object_name_pattern` as Regex pattern to extract the base filename from the output S3 key.
`s3_key_path` as JSON path in the incoming pipeline event that contains the S3 object key
`correlation_key` as the Field name used to correlate/match records between output and source files

##  Metrics
- 'numberOfRecordsEnrichedSuccessFromS3': Number of pipeline records successfully enriched from S3 source 
- 'numberOfRecordsEnrichedFailerFromS3': Number of pipeline records that failed enrichment from S3 source
- 's3EnricherObjectsFailed': Number of S3 source objects successfully loaded for enrichment
- 's3EnricherObjectsSucceeded': Number of S3 source objects that failed to load for enrichment

## Developer Guide

The integration tests for this plugin do not run as part of the Data Prepper build.
