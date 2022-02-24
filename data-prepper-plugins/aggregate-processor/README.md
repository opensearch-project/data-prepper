# Aggregate Processor

This stateful processor groups Events together based on the values of the [identification_keys](#identification_keys) provided, and performs a configurable [action](#action) on each group. You can use existing actions, or you can create your own actions as Java code to perform custom aggregations.
It is a great way to reduce unnecessary log volume and create aggregated logs over time.

## Usage

The following pipeline configuration will aggregate Events based on the entries with keys `sourceIp`, `destinationIp`, and `port`. It uses the [remove_duplicates](#remove_duplicates) action. 
While not necessary, a great way to set up the Aggregate Processor [identification_keys](#identification_keys) is with the [Grok Processor](../grok-prepper/README.md) as shown below.
```yaml
  source:
    ...
  prepper:
    - grok:
        match: 
          log: ["%{IPORHOST:sourceIp} %{IPORHOST:destinationIp} %{NUMBER:port:int}"]
          
    - aggregate:
        identification_keys: ["sourceIp", "destinationIp", "port"]
        action:
          remove_duplicates:
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
### <a name="group_duration"></a>
* `group_duration` (Optional): The amount of time, in seconds, that a group should exist before it is concluded automatically. Default value is `180`.

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

## Creating New Aggregate Actions

It is easy to create custom Aggregate Actions to be used by the Aggregate Processor. To do so, create a new class that implements the [AggregateAction interface](src/main/java/com/amazon/dataprepper/plugins/processor/aggregate/AggregateAction.java).
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
For actual examples, take a closer look at the code for some existing AggregateActions [here](src/main/java/com/amazon/dataprepper/plugins/processor/aggregate/actions).

## State

This processor holds the state for groups in memory. At the moment, state is not preserved across restarts of Data Prepper.
This functionality is on the Data Prepper Roadmap.

## Metrics

Apart from common metrics in [AbstractProcessor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/com/amazon/dataprepper/model/processor/AbstractProcessor.java), the Aggregate Processor introduces the following custom metrics.

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
