Attached simple-sample-pipeline: opensearch-data-prepper\data-prepper\data-prepper-plugins\s3-sink\src\main\resources\pipelines.yaml

Functional Requirements:
1	Provide a mechanism to received events from buffer then process and write to s3.
2	Codecs encode the events into the desired format based on the configuration.
3	Flush the encoded events into s3 bucket as objects.
4	Object name based on the key-pattern.
5	Object length depends on the thresholds provided in the configuration. 
6	The Thresholds such as events count, bytes capacity and data collection duration.
