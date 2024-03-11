# Data Prepper Log Ingestion from Fluentd

This is an example of using Fluentd to send data to Data Prepper and then to OpenSearch.

This is based on the existing [log-ingestion](../log-ingestion/README.md) sample.
The Data Prepper pipeline is also the same one from that example.

To run:

1. Run `docker compose up`
2. Wait for everything to come up.
3. Add log lines to the [`../log-ingestion/test.log`](../log-ingestion/test.log) file.

You can add these lines, just like in the FluentBit demo.
