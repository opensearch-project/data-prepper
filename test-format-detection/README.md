# Format Detection — Local Pipeline Test

## Prerequisites

1. An S3 bucket with test data files (JSON, NDJSON, CSV, Parquet, etc.)
2. An IAM role with `s3:GetObject` and `s3:ListBucket` permissions
3. AWS credentials configured locally (`~/.aws/credentials` or env vars)

## Setup

1. Edit the pipeline YAML to fill in your values:
   - `<YOUR_ACCOUNT_ID>` — your AWS account
   - `<YOUR_BUCKET_NAME>` — S3 bucket with test data
   - `<YOUR_ROLE_NAME>` — IAM role for S3 access
   - `<YOUR_QUEUE_NAME>` — SQS queue (only for sqs-pipeline)

2. Upload test data to S3:
   ```bash
   aws s3 cp test-format-detection/data/ s3://<YOUR_BUCKET>/test-data/ --recursive
   ```

## Run Data Prepper Locally

### Option A: S3 Scan (one-time, no SQS needed)

```bash
cd ~/data-prepper
./gradlew :data-prepper-main:build -x test

java -Ddata-prepper.dir=test-format-detection \
  -jar data-prepper-main/build/libs/data-prepper-main-*.jar \
  test-format-detection/pipelines/s3-scan-pipeline.yaml \
  test-format-detection/data-prepper-config.yaml
```

### Option B: S3-SQS (event-driven, needs SQS queue + S3 notifications)

```bash
java -Ddata-prepper.dir=test-format-detection \
  -jar data-prepper-main/build/libs/data-prepper-main-*.jar \
  test-format-detection/pipelines/s3-sqs-pipeline.yaml \
  test-format-detection/data-prepper-config.yaml
```

## What to Expect

The pipeline will:
1. Read each S3 object line-by-line (newline codec)
2. Run `detect_format` processor on the `message` field of each event
3. Add a `detected_format` field (json, xml, csv, keyvalue, or null)
4. Print each event to stdout

## Note

This uses the **existing** `detect_format` processor which works per-event (on already-parsed fields).
Our new `FormatDetector` library works at the **file level** (before parsing). To test file-level
detection, use the `FormatDetectorPipelineSimulator` instead:

```bash
./gradlew :data-prepper-plugins:format-detection:jar
java -cp data-prepper-plugins/format-detection/build/libs/data-prepper-format-detection-*.jar \
  org.opensearch.dataprepper.plugins.formatdetection.FormatDetectorPipelineSimulator \
  /path/to/local/data/
```
