/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.model.event;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static com.fasterxml.jackson.core.JsonPointer.SEPARATOR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Jackson Implementation of {@link Event} interface. This implementation relies heavily on JsonNode to manage the keys of the event.
 * <p>
 * This implementation supports dot-notation for keys to access nested structures. For example using the key "fizz.buzz" would allow a
 * user to retrieve the number 42 using {@link #get(String, Class)} from the nested structure below.
 * <p>
 *     {
 *         "foo": "bar"
 *         "fizz": {
 *             "buzz": 42
 *         }
 *     }
 *
 * @since 1.2
 */
public class JacksonEvent implements Event {

    private static final Logger LOG = LoggerFactory.getLogger(JacksonEvent.class);

    private static final String DOT_NOTATION_REGEX = "\\.";

    private static final ObjectMapper mapper = new ObjectMapper();

    private final EventMetadata eventMetadata;

    private final JsonNode jsonNode;

    private JacksonEvent(final Builder builder) {

        if (builder.eventMetadata == null) {
            this.eventMetadata = new DefaultEventMetadata.Builder()
                    .withEventType(builder.eventType)
                    .withTimeReceived(builder.timeReceived)
                    .withAttributes(builder.eventMetadataAttributes)
                    .build();
        } else {
            this.eventMetadata = builder.eventMetadata;
        }

        if (builder.data == null) {
            this.jsonNode = mapper.valueToTree(new HashMap<>());
        } else {
            this.jsonNode = mapper.valueToTree(builder.data);
        }

    }

    /**
     * Adds or updates the key with a given value in the Event.
     * @param key where the value will be set
     * @param value value to set the key to
     * @since 1.2
     */
    @Override
    public void put(final String key, final Object value) {

        checkKeyArgument(key);

        final LinkedList<String> keys = new LinkedList<>(Arrays.asList(key.split(DOT_NOTATION_REGEX)));

        JsonNode parentNode = jsonNode;

        while (!keys.isEmpty()) {
            if (keys.size() == 1) {
                final JsonNode valueNode = mapper.valueToTree(value);
                ((ObjectNode) parentNode).set(keys.removeFirst(), valueNode);
            } else {
                final String childKey = keys.removeFirst();
                JsonNode childNode = parentNode.get(toJsonPointerExpression(childKey));
                if (childNode == null) {
                    childNode = mapper.createObjectNode();
                    ((ObjectNode) parentNode).set(childKey, childNode);
                }
                parentNode = childNode;
            }
        }
    }

    /**
     * Retrieves the value of type clazz from the key.
     * @param key the value to retrieve from
     * @param clazz the return type of the value
     * @return the value
     * @throws RuntimeException if it is unable to map the value to the provided clazz
     * @since 1.2
     */
    @Override
    public <T> T get(final String key, final Class<T> clazz) {

        checkKeyArgument(key);

        final JsonPointer jsonPointer = toJsonPointer(key);

        JsonNode node = jsonNode.at(jsonPointer);
        if (node.isMissingNode()) {
            return null;
        }

        try {
            return mapper.treeToValue(node, clazz);
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to map {} to {}", key, clazz, e);
            throw new RuntimeException(String.format("Unable to map %s to %s", key, clazz), e);
        }
    }

    private String toJsonPointerExpression(final String key) {
        return key.replaceAll("\\.", "\\/");
    }

    private JsonPointer toJsonPointer(final String key) {
        String jsonPointerExpression = SEPARATOR + toJsonPointerExpression(key);
        return JsonPointer.compile(jsonPointerExpression);
    }

    /**
     * Deletes the key from the event.
     *
     * @param key the field to be deleted
     */
    @Override
    public void delete(final String key) {

        checkKeyArgument(key);
        final int index = key.lastIndexOf(".");

        JsonNode baseNode = jsonNode;
        String leafKey = key;

        if (index != -1) {
            final JsonPointer jsonPointer = toJsonPointer(key.substring(0, index));
            baseNode = jsonNode.at(jsonPointer);
            leafKey = key.substring(index + 1);
        }

        if (!baseNode.isMissingNode()) {
            ((ObjectNode) baseNode).remove(leafKey);
        }
    }

    @Override
    public String toJsonString() {
        return jsonNode.toString();
    }

    @Override
    public EventMetadata getMetadata() {
        return eventMetadata;
    }

    private void checkKeyArgument(final String key) {
        checkNotNull(key, "key cannot be null");
        checkArgument(!key.isEmpty(), "key cannot be an empty string");
        checkArgument(key.matches("^([a-zA-Z0-9]([\\w -]*[a-zA-Z0-9])+\\.?)+(?<!\\.)$"), "key must contain only alphanumeric chars with .- and  must follow dot notation (ie. 'field.to.key')");
    }

    /**
     * Builder for creating {@link JacksonEvent}.
     * @since 1.2
     */
    public static class Builder {
        private EventMetadata eventMetadata;
        private Object data;
        private String eventType;
        private Instant timeReceived;
        private Map<String, Object> eventMetadataAttributes;

        /**
         * Sets the event type for the metadata if a {@link #withEventMetadata} is not used.
         * @param eventType the event type
         * @since 1.2
         */
        public Builder withEventType(final String eventType) {
            this.eventType = eventType;
            return this;
        }

        /**
         * Sets the attributes for the metadata if a {@link #withEventMetadata} is not used.
         * @param eventMetadataAttributes the attributes
         * @since 1.2
         */
        public Builder withEventMetadataAttributes(final Map<String, Object> eventMetadataAttributes) {
            this.eventMetadataAttributes = eventMetadataAttributes;
            return this;
        }

        /**
         * Sets the time received for the metadata if a {@link #withEventMetadata} is not used.
         * @param timeReceived the time an event was received
         * @since 1.2
         */
        public Builder withTimeReceived(final Instant timeReceived) {
            this.timeReceived = timeReceived;
            return this;
        }

        /**
         * Sets the metadata.
         * @param eventMetadata the metadata
         * @since 1.2
         */
        public Builder withEventMetadata(final EventMetadata eventMetadata) {
            this.eventMetadata = eventMetadata;
            return this;
        }

        /**
         * Sets the data of the event.
         * @param data the data
         * @since 1.2
         */
        public Builder withData(final Object data) {
            this.data = data;
            return this;
        }

        /**
         * Returns a newly created {@link JacksonEvent}.
         * @return an event
         * @since 1.2
         */
        public JacksonEvent build() {
            return new JacksonEvent(this);
        }
    }
}
