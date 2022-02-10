# Aggregate Processor

TODO

## Metrics

Apart from common metrics in [AbstractProcessor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/com/amazon/dataprepper/model/processor/AbstractProcessor.java), the Aggregate Processor introduces the following custom metrics.

**Counter**

* `actionHandleEventsForwarded`: The number of Events that have been returned from the `handleEvent` call to the [action]() configured


* `actionHandleEventsDropped`: The number of Events that have not been returned from the `handleEvent` call to the [action]() configured.


* `actionHandleEventsProcessingErrors`: The number of calls made to `handleEvent` for the [action]() configured that resulted in an error.


* `actionConcludeGroupEventsForwarded`: The number of Events that have been returned from the `concludeGroup` call to the [action]() configured.


* `actionConcludeGroupEventsDropped`: The number of Events that have not been returned from the `condludeGroup` call to the [action]() configured.


* `actionConcludeGroupEventsProcessingErrors`: The number of calls made to `concludeGroup` for the [action]() configured that resulted in an error.

**Gauge**

* `currentAggregateGroups`: The current number of groups. This gauge decreases when groups are concluded, and increases when an Event triggers the creation of a new group.

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/readme/monitoring.md)
