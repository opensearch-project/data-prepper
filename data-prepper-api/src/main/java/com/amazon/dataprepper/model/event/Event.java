package com.amazon.dataprepper.model.event;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * All data flowing through Data Prepper will be represented as events. An event is the base representation of data.
 * An event can be defined as a collection of key-value pairs and the following interface represents the contract with this model.
 * <p>
 * Data Prepper will be migrating away from the original use of {@link com.amazon.dataprepper.model.record.Record}s.
 * Internal interfaces for {@link com.amazon.dataprepper.model.prepper.Prepper}, {@link com.amazon.dataprepper.model.buffer.Buffer},
 * {@link com.amazon.dataprepper.model.sink.Sink}, & {@link com.amazon.dataprepper.model.source.Source}. will be extended to support
 * the new internal model. The use of {@link com.amazon.dataprepper.model.record.Record}s will be deprecated in 2.0.
 * <p>
 * @since 1.2
 */
public interface Event {

    /**
     * Adds or updates the key with a given value in the Event
     *
     * @param key where the value will be set
     * @param value value to set the key to
     * @since 1.2
     */
    void put(String key, Object value);

    /**
     * Retrieves the given key from the Event
     *
     * @param key the value to retrieve from
     * @param clazz the return type of the value
     * @return T a clazz object from the key
     * @since 1.2
     */
    <T> T get(String key, Class<T> clazz);

    /**
     * Deletes the given key from the Event
     * @param key the field to be deleted
     * @since 1.2
     */
    void delete(String key);

    /**
     * Generates a Json representation of the entire Event
     * @return JsonNode of the event
     * @since 1.2
     */
    JsonNode toJsonNode();

    /**
     * Retrieves the EventMetadata
     * @return EventMetadata for the event
     * @since 1.2
     */
    EventMetadata getMetadata();

}
