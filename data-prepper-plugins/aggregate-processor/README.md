# Aggregate Processor

This stateful processor groups Events together based on the values of the [identification_keys](#identification_keys) provided, and performs a configurable [action](#action) on each group. You can use existing actions, or you can create your own actions as Java code to perform custom aggregations.
It is a great way to reduce unnecessary log volume and create aggregated logs over time.

## Basic Usage

The following pipeline configuration will aggregate Events based on the entries with keys `sourceIp`, `destinationIp`, and `port`. It uses the [remove_duplicates](#remove_duplicates) action. 
While not necessary, a great way to set up the Aggregate Processor [identification_keys](#identification_keys) is with the [Grok Processor](../grok-processor/README.md) as shown below.
```yaml
  source:
    ...
  processor:
    - grok:
        match: 
          log: ["%{IPORHOST:sourceIp} %{IPORHOST:destinationIp} %{NUMBER:port:int}"]
          
    - aggregate:
        identification_keys: ["sourceIp", "destinationIp", "port"]
        action:
          remove_duplicates:
        aggregate_when: "/sourceIp == 10.10.10.10"
  sink:
     ...
```

## Configuration

### Options

* [identification_keys](#identification_keys) (Required)
* [action](#action) (Required)
* [group_duration](#group_duration) (Optional)

### <a name="identification_keys"></a>
* `identification_keys` (Required): A non-ordered `List<String>` by which to group Events. Events with the same values for these keys are put into the same group. If an Event does not contain one of the `identification_keys`, then the value of that key is considered to be equal to `null`. At least one identification_key is required.

### <a name="action"></a>
* `action` (Required): The action to be performed for each group. One of the existing [Aggregate Actions](#available-aggregate-actions) must be provided.
    * [remove_duplicates](#remove_duplicates)
    * [put_all](#put_all)
    * [append](#append)
    * [count](#count)
    * [histogram](#histogram)
    * [rate_limiter](#rate_limiter)
    * [percent_sampler](#percent_sampler)
    * [tail_sampler](#tail_sampler)
### <a name="group_duration"></a>
* `group_duration` (Optional): A `String` that represents the amount of time that a group should exist before it is concluded automatically. Supports ISO_8601 notation Strings ("PT20.345S", "PT15M", etc.) as well as simple notation Strings for seconds ("60s") and milliseconds ("1500ms"). Default value is `180s`.

### <a name="when"></a>
* `when` (Optional): A `String` that represents a condition that must be evaluated to true for the aggregation to be applied on the event. Events that do not evaluate to true on the condition are skipped. Default is no condition which means all events are included in the aggregation.

## Available Aggregate Actions

### <a name="remove_duplicates"></a>
* `remove_duplicates`: Processes the first Event for a group immediately, and drops Events that follow for that group.
  * After the following Event is processed with `identification_keys: ["sourceIp", "destination_ip"]`:
      ```json
          { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "status": 200 }
      ```
    The following Event will be dropped by Data Prepper:
      ```json
         { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 1000 }
      ```
    While this Event will be processed by Data Prepper and create a new group (since the `sourceIp` is different):
      ```json
         { "sourceIp": "127.0.0.2", "destinationIp": "192.168.0.1", "bytes": 1000 }
      ```

### <a name="put_all"></a>
* `put_all`: Combine Events belonging to the same group by overwriting existing keys and adding non-existing keys (the equivalence of java `Map.putAll`). All Events that make up the combined Event will be dropped.
    * Given the following three Events with `identification_keys: ["sourceIp", "destination_ip"]`:
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "status": 200 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 1000 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "http_verb": "GET" }
      ```
      The following Event will be created and processed by the rest of the pipeline when the group is concluded:
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "status": 200, "bytes": 1000, "http_verb": "GET" }
      ```

### <a name="append"></a>
* `append`: Combine Events belonging to the same group by merging values of common keys into a list. All Events that make up the combined Event will be dropped.
  * It supports the following config option
    * `keys_to_append` (Optional): Name of keys to check for merging. Default action is to look for all keys. 
  * Given the following two Events with `identification_keys: ["sourceIp", "destination_ip"]` and `keys_to_append` unset:
    ```json lines
      { "firstString": "firstEventString", "firstArray": [1, 2, 3], "firstNumber": 1, "matchingNumber": 10, "matchingNumberEqual": 38947, "matchingStringEqual": "equalString", "matchingNumberArray": [20,21,22], "matchingNumberArrayEqual": [20,21,22], "matchingString": "StringFromFirstEvent", "matchingStringArray": ["String1", "String2"],  "matchingDeepArray": [[30,31,32]]}
      { "secondString": "secondEventString", "secondArray": [4, 5, 6], "secondNumber": 2, "matchingNumber": 11, "matchingNumberEqual": 38947, "matchingStringEqual": "equalString", "matchingNumberArray": [23,24,25], "matchingNumberArrayEqual": [20,21,22], "matchingString": "StringFromSecondEvent", "matchingStringArray": ["String3", "String4"], "matchingDeepArray": [[30,31,32]]}
    ```
    The following Event will be created and processed by the rest of the pipeline when the group is concluded:
    ```json
      { "firstString": "firstEventString", "firstArray": [1, 2, 3], "firstNumber": 1, "matchingNumber": [10, 11], "matchingNumberEqual": 38947, "matchingStringEqual": "equalString", "matchingNumberArray": [20, 21, 22, 23, 24, 25], "matchingNumberArrayEqual": [20, 21, 22, 20, 21, 22], "matchingString": ["StringFromFirstEvent", "StringFromSecondEvent"], "matchingStringArray": ["String1", "String2", "String3", "String4"], "matchingDeepArray": [[30, 31, 32], [30, 31, 32]]}
    ```
    Notice that it has all the fields from the first event. It appended the values from second event only if the field was also present in the first event.
    The values in a list are merely appended, so there can be duplicates. 

### <a name="count"></a>
* `count`: Count Events belonging to the same group and generate a new event with values of the identification keys and the count, indicating the number of events. All Events that make up the combined Event will be dropped.
    * It supports the following config options
       * `count_key`: key name to use for storing the count, default name is `aggr._count`
       * `start_time_key`: key name to use for storing the start time, default name is `aggr._start_time`
       * `output_format`: format of the aggregated event.
         * `otel_metrics` - Default output format. Outputs in otel metrics SUM type with count as value
         * `raw` - generates JSON with `count_key` field with count as value and `start_time_key` field with aggregation start time as value

    * Given the following three Events with `identification_keys: ["sourceIp", "destination_ip"]`:
      ```json
          { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "status": 200 }
          { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "status": 503 }
          { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "status": 400 }
      ```
      The following Event will be created and processed by the rest of the pipeline when the group is concluded:
      ```json
        {"isMonotonic":true,"unit":"1","aggregationTemporality":"AGGREGATION_TEMPORALITY_DELTA","kind":"SUM","name":"count","description":"Number of events","startTime":"2022-12-02T19:29:51.245358486Z","time":"2022-12-02T19:30:15.247799684Z","value":3.0,"sourceIp":"127.0.0.1","destinationIp":"192.168.0.1"}
      ```
      If raw output format is used, the following Event will be created and processed by the rest of the pipeline when the group is concluded:
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "aggr._count": 3, "aggr._start_time": "2022-11-05T23:28:31.916Z"}
      ```
    * When used in combination with the `aggregate_when` condition like "/status == 200", the above 3 events will generate the following event
      ```json
        {"isMonotonic":true,"unit":"1","aggregationTemporality":"AGGREGATION_TEMPORALITY_DELTA","kind":"SUM","name":"count","description":"Number of events","startTime":"2022-12-02T19:29:51.245358486Z","time":"2022-12-02T19:30:15.247799684Z","value":1.0,"sourceIp":"127.0.0.1","destinationIp":"192.168.0.1"}
      ```
      If raw output format is used, the following Event will be created and processed by the rest of the pipeline when the group is concluded:
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "aggr._count": 1, "aggr._start_time": "2022-11-05T23:28:31.916Z"}
      ```

### <a name="histogram"></a>
* `histogram`: Aggreates events belonging to the same group and generate a new event with values of the identification keys and histogram of the aggregated events based on a configured `key`. The histogram contains the number of events, sum, buckets, bucket counts, and optionally min and max of the values corresponding to the `key`. All events that make up the combined Event will be dropped.
    * It supports the following config options
       * `key`: name of the field in the events for which histogram needs to be generated
       * `generated_key_prefix`: key prefix to be used for all the fields created in the aggregated event. This allows the user to make sure that the names of the histogram event does not conflict with the field names in the event
       * `units`: name of the units for the values in the `key`
       * `record_minmax`: a boolean indicating if the histogram should include the min and max of the values during the aggregation duration
       * `buckets`: a list of buckets (values of type `double`) indicating the buckets in histogram
       * `output_format`: format of the aggregated event.
         * `otel_metrics` - Default output format. Outputs in otel metrics SUM type with count as value
         * `raw` - generates JSON with `count_key` field with count as value and `start_time_key` field with aggregation start time as value
    * Given the following four Events with `identification_keys: ["sourceIp", "destination_ip", "request"]`,  `key` as "latency", `buckets` as `[0.0, 0.25, 0.5]` :
      ```json
          { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "request" : "/index.html", "latency": 0.2 }
          { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "request" : "/index.html", "latency": 0.55}
          { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "request" : "/index.html", "latency": 0.25 }
          { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "request" : "/index.html", "latency": 0.15 }
      ```
      The following Event will be created and processed by the rest of the pipeline when the group is concluded:
      ```json
        {"max":0.55,"kind":"HISTOGRAM","buckets":[{"min":-3.4028234663852886E38,"max":0.0,"count":0},{"min":0.0,"max":0.25,"count":2},{"min":0.25,"max":0.50,"count":1},{"min":0.50,"max":3.4028234663852886E38,"count":1}],"count":4,"bucketCountsList":[0,2,1,1],"description":"Histogram of latency in the events","sum":1.15,"unit":"seconds","aggregationTemporality":"AGGREGATION_TEMPORALITY_DELTA","min":0.15,"bucketCounts":4,"name":"histogram","startTime":"2022-12-14T06:43:40.848762215Z","explicitBoundsCount":3,"time":"2022-12-14T06:44:04.852564623Z","explicitBounds":[0.0,0.25,0.5],"request":"/index.html","sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "key": "latency"}
      ```
      If raw output format is used, the following event will be created and processed by the rest of the pipeline when the group is concluded:
      ```json
        {"request":"/index.html","aggr._max":0.55,"aggr._min":0.15,"aggr._buckets":[0.0, 0.25, 0.5],"sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "aggr._bucket_counts":[0,2,1,1],"aggr._count":4,"aggr._key":"latency","aggr._startTime":"2022-12-14T06:39:06.081Z","aggr._sum":1.15}
      ```

### <a name="rate_limiter"></a>
* `rate_limiter`: Processes the events and controls the number of events aggregated per second. By default, the processor blocks if more events than allowed by the configured number of events are received. This behavior can be overwritten with a config option which drops any excess events received in a given time period.
    * It supports the following config options
       * `events_per_second`: Number of events allowed per second
       * `when_exceeds`: indicates what action to be taken when more number of events than the number of events allowed per second are received. Default value is `block` which means the processor blocks after max number allowed per second are allowed until the next time period. Other option is `drop` which drops the excess events received in the time period.
    * When the following three events arrive with in one second and the `events_per_second` is set 1 and `when_exceeds` set to `drop`
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "status": 200 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 1000 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "http_verb": "GET" }
      ```
      The following Event will be allowed, and no event is generated when the group is concluded
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "status": 200 }
      ```
    * When the three events arrive with in one second and the `events_per_second` is set 1 and `when_exceeds` is set to `block`, all three events are allowed.


### <a name="percent_sampler"></a>
* `percent_sampler`: Processes the events and controls the number of events aggregated based on the configuration. Only specified `percent` of the events are allowed and the rest are dropped.
    * It supports the following config options
       * `percent`: percent of events to be allowed during aggregation window
    * When the following four events arrive with in one aggregataion period and the `percent` is set 50
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 2500 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 500 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 1000 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 3100 }
      ```
      The following Events will be allowed, and no event is generated when the group is concluded
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 500 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 3100 }
      ```

### <a name="tail_sampler"></a>
* `tail_sampler`: The system processes incoming events and determines whether or not they should be allowed based on two criteria. The first criterion is based on whether or not an error condition is present. If any of the aggregated events meet this condition, then all events are allowed to be output. The second criterion is triggered when no error condition is specified or if it is false. In this case, only a subset of the events is allowed to pass through, determined by a probabilistic outcome based on the configured percent value. Since it is difficult to determine exactly when "tail sampling" should occur, the wait_period configuration parameter is used to determine when to conduct this sampling based on the idle time after the last received event. When this action is used, the aggregate `group_duration` is not relevant as the conclusion is based on the `wait_period` and not on the group duration.
    * It supports the following config options
       * `percent`: percent of events to be allowed during aggregation window
       * `wait_period`: minimum idle time before tail sampling is triggered
       * `error_condition`: optional condition to indicate the error case for tail sampling
    * When the following three events arrive with `percent` is set to 33, and no error condition specified (or error condition evaluates to false)
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 2500 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 500 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 1000 }
      ```
      The following Events may be allowed, and no event is generated when the group is concluded (Since this is probablistic sampling, exact output is fully deterministic)
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 500 }
      ```
    * When the following three events arrive with in one second and the `error_condition` is set to `/bytes > 3000`
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 2500 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 500 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 3100 }
      ```
      The following Events (all) will be allowed, and no event is generated when the group is concluded
      ```json
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 2500 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 500 }
        { "sourceIp": "127.0.0.1", "destinationIp": "192.168.0.1", "bytes": 3100 }
      ```

## Creating New Aggregate Actions

It is easy to create custom Aggregate Actions to be used by the Aggregate Processor. To do so, create a new class that implements the [AggregateAction interface](src/main/java/org/opensearch/dataprepper/plugins/processor/aggregate/AggregateAction.java).
The interface has the following signature:

```
public interface AggregateAction {

    // This function will be called once for every single Event. If this function throws an error, the Event passed will be returned and processed by the rest of the pipleine.
    default AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        return AggregateActionResponse.fromEvent(event);
    }
    
    // This function will be called once for a group. It will be called when the group_duration for that group has passed.
    default Optional<Event> concludeGroup(final AggregateActionInput aggregateActionInput) {
        return Optional.empty();
    }
}
```

The `AggregateActionInput` that is passed to the functions of the interface contains a method `getGroupState()`, which returns a `GroupState` Object that can be operated on like a java `Map`. 
For actual examples, take a closer look at the code for some existing AggregateActions [here](src/main/java/org/opensearch/dataprepper/plugins/processor/aggregate/actions).

## State

This processor holds the state for groups in memory. At the moment, state is not preserved across restarts of Data Prepper.
This functionality is on the Data Prepper Roadmap.

## Metrics

Apart from common metrics in [AbstractProcessor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/org/opensearch/dataprepper/model/processor/AbstractProcessor.java), the Aggregate Processor introduces the following custom metrics.

**Counter**

* `actionHandleEventsOut`: The number of Events that have been returned from the `handleEvent` call to the [action](#action) configured


* `actionHandleEventsDropped`: The number of Events that have not been returned from the `handleEvent` call to the [action](#action) configured.


* `actionHandleEventsProcessingErrors`: The number of calls made to `handleEvent` for the [action](#action) configured that resulted in an error.


* `actionConcludeGroupEventsOut`: The number of Events that have been returned from the `concludeGroup` call to the [action](#action) configured.


* `actionConcludeGroupEventsDropped`: The number of Events that have not been returned from the `condludeGroup` call to the [action](#action) configured.


* `actionConcludeGroupEventsProcessingErrors`: The number of calls made to `concludeGroup` for the [action](#action) configured that resulted in an error.

**Gauge**

* `currentAggregateGroups`: The current number of groups. This gauge decreases when groups are concluded, and increases when an Event triggers the creation of a new group.

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
