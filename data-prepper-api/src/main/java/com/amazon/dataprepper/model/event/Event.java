package com.amazon.dataprepper.model.event;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * All data flowing through Data Prepper are represented as events. An event is the base representation of data. An event
 * can be defined as a collection of key-value pairs and the following interface represents the contract with this model.
 */
public interface Event {

    /**
     * Adds or updates the key with a given value in the Event
     *
     * @param key where the value will be set
     * @param value value to set the key to
     */
    void put(String key, Object value);

    /**
     * Retrieves the given key from the Event
     *
     * @param key the value to retrieve from
     * @param clazz the return type of the value
     * @return T a clazz object from the key
     */
    <T> T get(String key, Class<T> clazz);

    /**
     * Deletes the given key from the Event
     * @param key the field to be deleted
     */
    void delete(String key);

    /**
     * Generates a Json representation of the entire Event
     * @return JsonNode of the event
     */
    JsonNode toJsonNode();

    /**
     * Retrieves the EventMetadata
     * @return EventMetadata for the event
     */
    EventMetadata getMetadata();

}
