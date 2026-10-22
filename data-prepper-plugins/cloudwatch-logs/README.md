# CloudWatch Logs Sink

This Data Prepper sink allows the sending of log data to CloudWatch Logs via a CloudWatchLogsClient.

## Usages

The CloudWatch Logs sink should be configured as part of Data Prepper pipeline yaml file.

## Configuration Options

```
pipeline:
  ...
  sink:
    - cloudwatch_logs:
        aws:
          region: us-east-1
          sts_role_arn: arn:aws:iam::123456789012:role/Data-Prepper
          sts_header_overrides:
            custom_header: ...
            custom_header2: ...
            ...
          sts_external_id: 123ABC
        log_group: sample_group
        log_stream: sample_stream
        buffer_type: in_memory
        threshold:
          batch_size: 10000
          max_event_size: 256kb
          max_request_size: 1mb
        max_retries: 5
        workers: 10
        header_overrides:
          X-Custom-Header: "custom-value"
          X-Request-ID: "request-123"
          X-Source: "dataprepper"
        endpoint: "https://logs.us-west-2.amazonaws.com"
        create_log_group: false
        create_log_stream: false
        entity:
          key_attributes:
            Type: "RemoteService"
            Name: "okta_auth0"
          attributes:
            AWS.ServiceNameSource: "UserConfiguration"
```

## AWS Configuration

- `region` (Optional) : A string representing the AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).

- `sts_role_arn` (Optional) : A string representing AWS STS role to assume for requests to CloudWatchLogs. which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html).

- `sts_header_overrides` (Optional) : A string map representing different custom headers that can be added.

- `sts_external_id` (Optional) : A string representing the external ID to attach to AssumeRole requests. Referenced here: [how to use external ID](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)

## Threshold Configuration

- `batch_size` (Optional) : An integer value that indicates how many events we hold until we make a call to CloudWatch Logs. Defaults to 25. (Min = 1, Max = 10000)

- `max_event_size` (Optional) : A string representing the max size in bytes of the allowed events. Defaults to "256kb". (Min = "1b", Max = "256kb")

- `max_request_size` (Optional) : A string representing the count or size of bytes we hold until we make a call to CloudWatch Logs. Default is "1mb". (Min = "1b", Max = "1mb")

- `log_send_interval` (Optional) : A string representing the amount of time in seconds between making requests. Defaults to "60s". (Min = "5s", Max = "300s") 

- `header_overrides` (Optional) : A string map representing custom HTTP headers to include in CloudWatch Logs requests.

- `endpoint` (Optional) : A string representing a custom CloudWatch Logs endpoint URL to override the default service endpoint.

- `max_retries` (Optional) : An integer value that indicates the number of retries made for each `PutLogEvents` request after its first attempt, so a request makes at most `1 + max_retries` calls. It is always applied, with or without a `dlq`. Defaults to 5. (Min = 1, Max = 15)

- `workers` (Optional) : An integer cap on the number of concurrent `PutLogEvents` calls per sink instance. Defaults to 10. (Min = 1, Max = 50)

- `dlq` (Optional) : A DLQ plugin configuration. Events that cannot be delivered are written to the DLQ, see [Delivery semantics](#delivery-semantics).

- `create_log_group` (Optional): A boolean that controls whether the sink will
  create the configured `log_group` if it does not already exist. Defaults to
  `false`. When set to `true`, the IAM principal must have `logs:CreateLogGroup`
  permission.

- `create_log_stream` (Optional): A boolean that controls whether the sink will
  create the configured `log_stream` if it does not already exist. Defaults to
  `false`. When set to `true`, the IAM principal must have `logs:CreateLogStream`
  permission.

## Entity Configuration

- `entity` (Optional) : An object that attaches CloudWatch [Entity](https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_Entity.html) metadata to every `PutLogEvents` request. When omitted, no entity is attached and behavior is unchanged.

  - `key_attributes` (Required when `entity` is configured) : A `Map<String, String>` of key attributes that uniquely identify the entity (for example `Type`, `Name`).
  - `attributes` (Optional) : A `Map<String, String>` of additional attributes describing the entity. Defaults to an empty map.
  - `max_cardinality` (Optional) : An integer bounding how many entities are buffered **concurrently** in dynamic mode. Defaults to 1000. (Min = 1). Ignored in static mode, where there is only ever one entity.

  Each concurrently buffered entity holds its own buffer, so `max_cardinality` multiplied by `max_request_size` is the worst-case buffered footprint — that is what to size it against. A group is released once its buffer has been drained and it has been idle past `log_send_interval`, so the bound applies to entities active at one time rather than to how many distinct keys have ever been seen. If the bound is reached anyway, further distinct keys collapse into a shared fallback group carrying no per-resource entity; that is logged once and counted by `cloudWatchLogsEntityOverflowEvents`.

  ### Static vs dynamic entities

  The entity is **dynamic** when any `key_attributes` or `attributes` value contains a `${...}` reference — either a Data Prepper field reference such as `${/resourceId}` or an expression such as `${getMetadata("resourceId")}`; otherwise it is **static**. No separate mode flag is required.

  - Static: one constant entity is attached to every `PutLogEvents` request, exactly as configured.
  - Dynamic: each value is interpolated against the event, and the sink partitions buffered events by their resolved key attributes, sending one `PutLogEvents` request per distinct entity. This is how a stream carrying logs for many resources (for example Azure diagnostic logs keyed by `resourceId`) gets one correct entity per resource. If a resolved value is blank or longer than 512 characters, the event is sent without an entity.

  ```yaml
  entity:
    key_attributes:
      Type: "Azure::Resource"
      Identifier: "${/resourceId}"
    max_cardinality: 2000
  ```

  AWS-owned limits — maximum number of entries, allowed key names (such as `Type`, `ResourceType`, `Identifier`, `Name`, `Environment`), and key/value length caps — are enforced by CloudWatch at request time and are intentionally not mirrored here. Violations are surfaced via the `cloudWatchLogsEntityRejected` metric and a log warning. See the [AWS Entity API docs](https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_Entity.html) for the current contract.

## Buffer Type Configuration

- `buffer_type` (Optional) : A string representing the type of buffer to use to hold onto events. Currently only supports `in_memory`.

## Plugin Functionality
The cloudwatch_logs sink plugin uses credentials to establish a client to CloudWatch Logs. It currently uses the current system timestamp for publishing and implements an exponential back off strategy
with full jitter for retransmission.

The cloudwatch_logs sink plugin also adds an overhead of 26 bytes added to each event message. This is done by the AWS SDK when formatting the API call to CloudWatch Logs. This must be considered when setting custom
threshold parameters.

## Delivery semantics

Each sink call (`doOutput`) buffers its events, takes the batches that are ready, sends them on a pool of `workers` threads and
waits until every batch it took has been delivered, redriven or written to the DLQ. The pipeline's
`<pipeline>.<sink>.timeElapsed` timer measures that call and so includes delivery time; the sink adds no separate flush timer.

Each `PutLogEvents` request makes at most `1 + max_retries` calls. SDK-level retries are disabled; each attempt times out
after 10 s and each call after 15 s (`T_call`). Every error is retried. Before retry *n* the sink sleeps a random time in
[0, min(30 s, 500 ms × 2^(n−1))]. When `create_log_group` or `create_log_stream` is set, a missing log group or stream is
created at most once per request and counts as a failed attempt.

An event that cannot be delivered reaches one terminal step:

| Reason | With `dlq` | Without `dlq` |
|---|---|---|
| `undeliverable` (larger than `max_event_size`) | written to the DLQ | dropped (`release(true)`) |
| `rejected` (in `rejectedLogEventsInfo`) | written to the DLQ | dropped (`release(true)`) |
| `retriesExhausted`, `unexpectedError`, `shutdown` | written to the DLQ | negatively acknowledged (`release(false)`), so the source redrives it |

If the DLQ write fails, the events are negatively acknowledged (reason `dlqWriteFailed`). This includes a DLQ plugin that
supplied no writer: its write returns `false` and is handled the same way.

Redrive requires end-to-end acknowledgements. With acknowledgements disabled, every negative acknowledgement above is a drop,
made visible by `cloudWatchLogsEventsNegativelyAcked`. Redrive is coarse-grained for the supported sources: Kafka seeks the
whole partition back to its last committed offset, and `azure_blob` gives up the whole partition. Without a DLQ, a persistent
failure that is not `undeliverable` replays the partition indefinitely and duplicates events that were already delivered.
Operators should alarm on `cloudWatchLogsEventsNegativelyAcked{reason=retriesExhausted}`.

### Time bounds

Worst-case time for one request, where `C` = 1 if `create_log_group` or `create_log_stream` is true, else 0:

`T_send ≤ (1 + max_retries) × T_call + 2 × C × T_call + Σ_{n=1..max_retries} min(30 s, 0.5 s × 2^(n−1))`

With `C` = 1, `max_retries = 15` gives about 9.5 min and the default `max_retries = 5` gives about 2.3 min. `T_send` excludes
the DLQ write.

Worst-case time from an event reaching the sink to its release:

`T_event ≤ flush_interval + delay + ceil(B / workers) × (T_send + T_dlq)`

where `delay` is the pipeline buffer read timeout (default 3 s), `T_dlq` is the DLQ write time and `B` is the number of batches
queued on the pool ahead of and including the event's batch (up to the pipeline's sink threads × (`max_cardinality` + 1 + size
splits)).

Set the source acknowledgement timeout to at least `T_event` for your configuration, or leave it at the source default
(Kafka's default is effectively unbounded). An acknowledgement set that expires while the sink still holds its events causes a
redrive, which duplicates events. With `invokeCallbackOnExpiry`, a later release can invoke the callback a second time.

Note for Bifrost (`max_retries` 15, `create_log_stream` true, `T_call` 15 s), during a full outage: `T_send` is about 9.5 min
worst case and about 2.5 min when errors return quickly. There is no fail-fast across batches, so every batch spends its full
`T_send`, and at full cardinality `T_event` can reach many hours (`ceil(B / workers)` = 10 already gives about 95 min). For
Kafka this only delays the redrive. `azure_blob`'s acknowledgement timeout (about 2 h) can expire first and cause duplicate
redrives; events are duplicated, not lost. Lowering `max_retries` shortens both bounds.

### Flush latency

Readiness is checked on every sink call, including calls with no events; there is no timer thread. A group is flushed when it
reaches `batch_size` or `max_request_size`, or when `flush_interval` has passed since it was created or last flushed. Effective
flush latency is therefore at most `flush_interval` + `delay` + the time all sink threads are blocked sending.

### Shutdown

On shutdown the sink stops accepting events, flushes every buffered event, and gives each batch at most one more attempt.
Attempts that would start more than 30 s after shutdown began are skipped, and those events take the `shutdown` row above.
Events received after shutdown also take the `shutdown` row. The sink then waits up to 20 s for its threads to finish and logs
an error if any are still running; those threads still settle their batches when they finish.

Data Prepper core can call the sink concurrently with shutdown and after it. `Pipeline.shutdown` shuts down the processor
executor, then calls the sink's `shutdown`, and only then shuts down the executor that runs sink calls. Process workers waiting
on a sink call ignore the interrupt and abandon it, so the sink call keeps running; a sink call interrupted by that executor's
`shutdownNow` keeps waiting for its batches, each of which makes at most one more attempt. When shutdown is triggered after a
sink call throws an `Error`, it runs on a sink thread, and core then waits its full `sinkShutdownTimeout` before `shutdownNow`.

## Metrics

### Counters

* `cloudWatchLogsEventsSucceeded` - The number of log events successfully published to CloudWatch Logs. Events rejected in `rejectedLogEventsInfo` are not counted here.
* `cloudWatchLogsEventsFailed` - The number of log events in requests that failed after all attempts or were stopped by shutdown.
* `cloudWatchLogsEventsNegativelyAcked` - The number of events negatively acknowledged, tagged with `reason` (`retriesExhausted`, `unexpectedError`, `dlqWriteFailed` or `shutdown`).
* `cloudWatchLogsEventsSentToDlq` - The number of events written to the DLQ, tagged with `reason` (`undeliverable`, `rejected`, `retriesExhausted`, `unexpectedError` or `shutdown`).
* `cloudWatchLogsEventsReleasedWithoutDelivery` - The number of events dropped without a DLQ, tagged with `reason` (`undeliverable` or `rejected`).
* `cloudWatchLogsRequestsSucceeded` - The number of log requests successfully made to CloudWatch Logs.
* `cloudWatchLogsRequestsFailed` - The number of log requests failed to reach CloudWatch Logs.
* `cloudWatchLogsEntityRejected` - The number of `PutLogEvents` responses where CloudWatch rejected the configured entity. The request itself still succeeds and events are released; this counter is the primary signal for misconfigured `entity` attributes.
* `cloudWatchLogsUnhandledError` - The number of times an unexpected error escaped the normal retry/DLQ path, counted once per batch or sink call. Distinct from `cloudWatchLogsEventsFailed`; non-zero values indicate a logic bug, classpath linkage failure, or response-handling error rather than transient API failure.
* `cloudWatchLogsThrottled` - The number of `PutLogEvents` attempts that failed due to throttling (HTTP 429). Each retry that receives a throttle response increments this counter independently.
* `cloudWatchLogsAccessDenied` - The number of requests or resource-creation attempts that failed due to insufficient IAM permissions. Covers both `PutLogEvents` access-denied responses and `CreateLogGroup`/`CreateLogStream` permission failures when `create_log_group` or `create_log_stream` is enabled.
* `cloudWatchLogsResourceNotFound` - The number of `PutLogEvents` attempts that failed because the target log group or log stream does not exist. Only incremented when `create_log_group` and `create_log_stream` are both disabled — if the sink is configured to create resources, a resource-not-found error is internal control flow rather than an actionable user signal.
* `cloudWatchLogsEntityGroupsCreated` - The number of buffer groups opened for a distinct resolved entity (dynamic entity mode only). Because idle groups are released and a returning entity opens a new group, this counts group churn over time; use `cloudWatchLogsEntityCardinality` for how many are active now. The shared overflow group is excluded so this stays a count of real entities.
* `cloudWatchLogsEntityOverflowEvents` - The number of events that could not be given their own entity group because the concurrent-entity limit was already reached, and were sent in the shared fallback group with no entity instead. Any non-zero value means events are losing their per-resource entity attribution; reduce the cardinality of the templated `key_attributes`, or raise `max_cardinality`.
* `cloudWatchLogsEntityDropped` - The number of events sent without an entity because a resolved value was blank or longer than 512 characters, tagged with `reason` (`blank_value` or `value_too_long`).

The sink does not emit `cloudWatchLogsEventsRejected`, `cloudWatchLogsRequestsInRetry`, `cloudWatchLogsOldestRetryAgeSeconds`,
`cloudWatchLogsUnretriedFailures` or `cloudWatchLogsEffectiveMaxRetries`.

### Gauges

* `cloudWatchLogsRequestsInFlight` - The number of `PutLogEvents` calls in progress.
* `cloudWatchLogsEntityCardinality` - The number of entity buffer groups currently held, i.e. the entities being buffered right now (dynamic entity mode only). The shared overflow group is excluded, so this too stays a count of real entities. This rises and falls with live cardinality, and sitting at `max_cardinality` alongside a climbing `cloudWatchLogsEntityOverflowEvents` means events are collapsing into the fallback group.

## Developer Guide

This plugin is compatible with Java 11. See below

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
