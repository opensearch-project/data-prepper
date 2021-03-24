# OTel Trace Group Prepper

This is a prepper that fills in the missing trace group related fields in the collection of raw span string records output by [otel-trace-raw-prepper](../dataPrepper-plugins/otel-trace-raw-prepper) and then convert them back into a new collection of string records.
It finds the missing trace group info for a spanId by looking up the relevant fields in its root span stored in opendistro-for-elasticsearch (ODFE) or Amazon Elasticsearch Service backend that the local data-prepper host ingest into. 