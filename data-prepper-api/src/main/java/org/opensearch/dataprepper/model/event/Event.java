/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * All data flowing through Data Prepper will be represented as events. An event is the base representation of data.
 * An event can be defined as a collection of key-value pairs and the following interface represents the contract with this model.
 * <p>
 * Data Prepper will be migrating away from the original use of {@link org.opensearch.dataprepper.model.record.Record}s.
 * Internal interfaces for {@link org.opensearch.dataprepper.model.processor.Processor}, {@link org.opensearch.dataprepper.model.buffer.Buffer},
 * {@link org.opensearch.dataprepper.model.sink.Sink} and {@link org.opensearch.dataprepper.model.source.Source} will be extended to support
 * the new internal model. The use of {@link org.opensearch.dataprepper.model.record.Record}s will be deprecated in 2.0.
 * <p>
 *
 * @since 1.2
 */
public interface Event extends Serializable {

    /**
     * Adds or updates the key with a given value in the Event
     *
     * @param key where the value will be set
     * @param value value to set the key to
     * @since 2.8
     */
    void put(EventKey key, Object value);

    /**
     * Adds or updates the key with a given value in the Event
     *
     * @param key   where the value will be set
     * @param value value to set the key to
     * @since 1.2
     */
    void put(String key, Object value);

    /**
     * Retrieves the given key from the Event
     *
     * @param key the value to retrieve from
     * @param clazz the return type of the value
     * @param <T> The type
     * @return T a clazz object from the key
     * @since 2.8
     */
    <T> T get(EventKey key, Class<T> clazz);

    /**
     * Retrieves the given key from the Event
     *
     * @param key   the value to retrieve from
     * @param clazz the return type of the value
     * @param <T>   The type
     * @return T a clazz object from the key
     * @since 1.2
     */
    <T> T get(String key, Class<T> clazz);

    /**
     * Retrieves the given key from the Event as a List
     *
     * @param key   the value to retrieve from
     * @param clazz the return type of elements in the list
     * @param <T>   The type
     * @return {@literal List<T>} a list of clazz elements
     * @since 2.8
     */
    <T> List<T> getList(EventKey key, Class<T> clazz);

    /**
     * Retrieves the given key from the Event as a List
     *
     * @param key   the value to retrieve from
     * @param clazz the return type of elements in the list
     * @param <T>   The type
     * @return {@literal List<T>} a list of clazz elements
     * @since 1.2
     */
    <T> List<T> getList(String key, Class<T> clazz);

    /**
     * Deletes the given key from the Event
     *
     * @param key the field to be deleted
     * @since 2.8
     */
    void delete(EventKey key);

    /**
     * Deletes the given key from the Event
     *
     * @param key the field to be deleted
     * @since 1.2
     */
    void delete(String key);

    /**
     * Delete all keys from the Event
     * @since 2.8
     */
    void clear();

    /**
     * Merges another Event into the current Event.
     * The values from the other Event will overwrite the values in the current Event for all keys in the current Event.
     * If the other Event has keys that are not in the current Event, they will be unmodified.
     *
     * @param other the other Event to merge into this Event
     * @throws IllegalArgumentException if the input event is not compatible to merge.
     * @throws UnsupportedOperationException if the current Event does not support merging.
     * @since 2.11
     */
    void merge(Event other);

    /**
     * Generates a serialized Json string of the entire Event
     *
     * @return Json string of the event
     * @since 1.2
     */
    String toJsonString();

    /**
     * Returns the JsonNode containing the internal representation of the event
     *
     * @return JsonNode
     * @since 2.5
     */
    JsonNode getJsonNode();

    /**
     * Gets a serialized Json string of the specific key in the Event
     *
     * @param key the field to be returned
     * @return Json string of the field
     * @since 2.8
     */
    String getAsJsonString(EventKey key);

    /**
     * Gets a serialized Json string of the specific key in the Event
     *
     * @param key the field to be returned
     * @return Json string of the field
     * @since 2.2
     */
    String getAsJsonString(String key);

    /**
     * Retrieves the EventMetadata
     *
     * @return EventMetadata for the event
     * @since 1.2
     */
    EventMetadata getMetadata();

    /**
     * Checks if the key exists.
     *
     * @param key name of the key to look for
     * @return returns true if the key exists, otherwise false
     * @since 2.8
     */
    boolean containsKey(EventKey key);

    /**
     * Checks if the key exists.
     *
     * @param key name of the key to look for
     * @return returns true if the key exists, otherwise false
     * @since 1.2
     */
    boolean containsKey(String key);

    /**
     * Checks if the value stored for the key is list
     *
     * @param key name of the key to look for
     * @return returns true if the key is a list, otherwise false
     * @since 2.8
     */
    boolean isValueAList(EventKey key);

    /**
     * Checks if the value stored for the key is list
     *
     * @param key name of the key to look for
     * @return returns true if the key is a list, otherwise false
     * @since 1.2
     */
    boolean isValueAList(String key);

    /**
     * @return a Map representation of the Event
     * @since 1.3
     */
    Map<String, Object> toMap();

    /**
     * Returns formatted parts of the input string replaced by their values in the event
     *
     * @param format input format
     * @return returns a string with no formatted parts, returns null if no value is found
     * @throws RuntimeException if the input string is not properly formatted
     * @since 2.1
     */
    String formatString(final String format);

    /**
     * Returns formatted parts of the input string replaced by their values in the event or the values from the result
     * of a Data Prepper expression
     * @param format input format
     * @param expressionEvaluator - The expression evaluator that will support formatting from Data Prepper expressions
     * @return returns a string with no formatted parts, returns null if no value is found
     * @throws RuntimeException if the input string is not properly formatted
     * @since 2.1
     */
    String formatString(final String format, final ExpressionEvaluator expressionEvaluator);

    /**
     * Returns formatted parts of the input string replaced by their values in the event or the values from the result
     * of a Data Prepper expression
     * @param format input format
     * @param expressionEvaluator - The expression evaluator that will support formatting from Data Prepper expressions
     * @param defaultValue - The String to use as a replacement for when keys in Events can't be found
     * @return returns a string with no formatted parts, returns null if no value is found
     * @throws RuntimeException if the input string is not properly formatted
     * @since 2.1
     */
    String formatString(final String format, final ExpressionEvaluator expressionEvaluator, final String defaultValue);

    /**
     * Returns event handle
     *
     * @return returns the event handle associated with the event
     * @since 2.2
     */
    EventHandle getEventHandle();

    default void putIfAbsent(final String key, final Class clazz, final Object value) {
        if (get(key, clazz) == null)
            put(key, value);
    }

    JsonStringBuilder jsonBuilder();

    abstract class JsonStringBuilder {
        private String tagsKey;

        private String rootKey;

        private List<String> includeKeys;

        private List<String> excludeKeys;

        /**
         * @param key key to be used for tags
         * @return JsonStringString with tags included
         * @since 2.3
         */
        public JsonStringBuilder includeTags(String key) {
            this.tagsKey = key;
            return this;
        }

        /**
         * @param rootKey key to be used for tags
         * @return JsonStringString with tags included
         * @since 2.4
         */
        public JsonStringBuilder rootKey(String rootKey) {
            this.rootKey = rootKey;
            return this;
        }

        /**
         * @param includeKeys A list of keys to be retained
         * @return JsonStringString with retained keys only
         * @since 2.4
         */
        public JsonStringBuilder includeKeys(List<String> includeKeys) {
            this.includeKeys = includeKeys;
            return this;
        }

        /**
         * @param excludeKeys A list of keys to be excluded
         * @return JsonStringString without excluded keys
         * @since 2.4
         */
        public JsonStringBuilder excludeKeys(List<String> excludeKeys) {
            this.excludeKeys = excludeKeys;
            return this;
        }

        /**
         * @return key used for tags
         * @since 2.3
         */
        public String getTagsKey() {
            return tagsKey;
        }

        /**
         * @return root key
         * @since 2.4
         */
        public String getRootKey() {
            return rootKey;
        }

        /**
         * @return a list of keys to be retrained.
         * @since 2.4
         */
        public List<String> getIncludeKeys() {
            return includeKeys;
        }

        /**
         * @return a list of keys to be excluded
         * @since 2.4
         */
        public List<String> getExcludeKeys() {
            return excludeKeys;
        }

        /**
         * @return json string
         * @since 2.3
         */
        public abstract String toJsonString();
    }
}
