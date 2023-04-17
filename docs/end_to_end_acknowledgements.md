
## End-to-end Acknowledgements
The Data Prepper framework offers a reliable way for sources to ensure the durability and reliability of data by tracking its delivery to sinks through end-to-end acknowledgements. If the pipeline has multiple sinks, including other pipeline sinks, the event level acknowledgements are sent only after the event is sent to all final sinks. The framework enables a source to create an acknowledgement set to monitor a batch of events and receive a positive acknowledgement when those events are successfully pushed to the sinks or negative acknowledgement when any one of the events could not be pushed to the sinks for any reason. In the event of a failure or crash of any component of the Data Prepper, if it is unable to send an event, the source will not receive an acknowledgement. In this case, the source will time out and can take necessary actions like retrying or logging the failure.

### Limitations
The end-to-end acknowledgement framework loses track of events sent to a different DataPrepper instance running on a different node, making it unsuitable for use when [PeerForwarder](https://github.com/opensearch-project/data-prepper/tree/main/docs/peer_forwarder.md) is used.
As of DataPrepper version 2.2, only S3 Source and OpenSearch Sink support end-to-end acknowledgements. End-to-end acknowledgements can be enabled for S3 source using `acknowledgements: true` config option.

The following sections explain how to add end-to-end acknowledgements support to either new or other existing sources and sinks.

### How to enable end-to-end acknowledgements for a source
1. Source plugin should create an acknowledgement set. An example way of creating an acknowledgement set is
   ```
   AcknowledgementSet acknowledgementSet = AcknowledgementSetManager.create((result)-> {
    // callback function code
    if (result) { // positive acknowledgement
       // do something
    } else {
       // do something else
    }, Duration.ofSeconds(timeout)); 
    // callback function is called after acknowledgements from all the events added to the acknowledgementSet are received
    // timeout - max wait time for receiving acknowledgements
   ```

2. Source should then add the events created to this `acknowledgementSet`.
An example way of adding events to the `acknowledgementSet` is
    ```
    for (Record record: records) {
        acknowledgementSet.add((Event)record.getData());
    }
    ```

### How to add support for end-to-end acknowledgements in a sink
After successfully sending the events to the external sink, the DataPrepper sink plugin should just issue release on each of the event's handle. An example code is
    ```
    public void output(final Collection<Record<Event>> records) {
        // send to sink. Capture the sink send status in "result"
        result = sendToSink();

        EventHandle eventHandle = ((Event)record.getData()).getEventHandle();
        // a result value of "false" would send negative acknowledgement
        eventHandle.release(result); 
    }
    ```

If the sink supports DLQ, then the example code is
    ```
    public void output(final Collection<Record<Event>> records) {
        // send to sink. Capture the sink send status in "result"
        result = sendToSink();
        if (!result) {
            result = sendToDLQ();
        }
            
        EventHandle eventHandle = ((Event)record.getData()).getEventHandle();
        // a result value of "false" would send negative acknowledgement
        eventHandle.release(status); 
    }
    ```

If the sink batches events before "flushing" to the sink, the sink may choose to just keep the `EventHandle` associated with an event and release it's reference on the event. See [OpenSearchSink](https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/opensearch) as an example for doing this way.


