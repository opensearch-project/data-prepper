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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Jackson Implementation of {@link Event} interface. This implementation relies heavily on JsonNode to manage the keys of the event.
 * <p>
 * This implementation supports [JsonPointer](https://datatracker.ietf.org/doc/html/rfc6901) for keys to access nested structures.
 * For example using the key "fizz/buzz" would allow a user to retrieve the number 42 using {@link #get(String, Class)} from the nested structure below.
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

    private static final String SEPARATOR = "/";

    private static final ObjectMapper mapper = new ObjectMapper();

    private final EventMetadata eventMetadata;

    private final JsonNode jsonNode;

    protected JacksonEvent(final Builder builder) {

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

        final String trimmedKey = checkAndTrimKey(key);

        final LinkedList<String> keys = new LinkedList<>(Arrays.asList(trimmedKey.split(SEPARATOR)));

        JsonNode parentNode = jsonNode;

        while (!keys.isEmpty()) {
            if (keys.size() == 1) {
                setNode(parentNode, keys.removeFirst(), value);
            } else {
                final String childKey = keys.removeFirst();
                if (!childKey.isEmpty()) {
                    parentNode = getOrCreateNode(parentNode, childKey);
                }
            }
        }
    }

    private void setNode(final JsonNode parentNode, final String leafKey, final Object value) {
        final JsonNode valueNode = mapper.valueToTree(value);
        if (isNumeric(leafKey)) {
            ((ArrayNode) parentNode).set(Integer.parseInt(leafKey), valueNode);
        } else {
            ((ObjectNode) parentNode).set(leafKey, valueNode);
        }
    }

    private JsonNode getOrCreateNode(final JsonNode node, final String key) {
        JsonNode childNode = node.get(key);
        if (childNode == null) {
            childNode = mapper.createObjectNode();
            ((ObjectNode) node).set(key, childNode);
        }
        return childNode;
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

        final String trimmedKey = checkAndTrimKey(key);

        final JsonPointer jsonPointer = toJsonPointer(trimmedKey);

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

    private JsonPointer toJsonPointer(final String key) {
        String jsonPointerExpression = SEPARATOR + key;
        return JsonPointer.compile(jsonPointerExpression);
    }

    /**
     * Deletes the key from the event.
     *
     * @param key the field to be deleted
     */
    @Override
    public void delete(final String key) {

        final String trimmedKey = checkAndTrimKey(key);
        final int index = trimmedKey.lastIndexOf(SEPARATOR);

        JsonNode baseNode = jsonNode;
        String leafKey = trimmedKey;

        if (index != -1) {
            final JsonPointer jsonPointer = toJsonPointer(trimmedKey.substring(0, index));
            baseNode = jsonNode.at(jsonPointer);
            leafKey = trimmedKey.substring(index + 1);
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

    private String checkAndTrimKey(final String key) {
        checkKey(key);
        return trimKey(key);
    }

    private void checkKey(final String key) {
        checkNotNull(key, "key cannot be null");
        checkArgument(!key.isEmpty(), "key cannot be an empty string");
        checkArgument(key.matches(
                        "^/?((([a-zA-Z][a-zA-Z0-9-_.]+[a-zA-Z0-9])|\\d)/?)+$"),
                String.format("key %s must contain only alphanumeric chars with .-_ and must follow JsonPointer (ie. 'field/to/key')", key));
    }

    private String trimKey(final String key) {

        final String trimmedLeadingSlash = key.startsWith(SEPARATOR) ? key.substring(1) : key;
        return trimmedLeadingSlash.endsWith(SEPARATOR) ? trimmedLeadingSlash.substring(0, trimmedLeadingSlash.length() - 2) : trimmedLeadingSlash;
    }

    private boolean isNumeric(final String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch(final NumberFormatException e){
            return false;
        }
    }

    /**
     * Constructs an empty builder.
     * @return a builder
     * @since 1.2
     */
    public static Builder builder() {
        return new Builder() {
            @Override public Builder getThis() {
                return this;
            }
        };
    }

    /**
     * Builder for creating {@link JacksonEvent}.
     * @since 1.2
     */
    public abstract static class Builder<T extends Builder<T>> {
        private EventMetadata eventMetadata;
        private Object data;
        private String eventType;
        private Instant timeReceived;
        private Map<String, Object> eventMetadataAttributes;

        public abstract T getThis();

        /**
         * Sets the event type for the metadata if a {@link #withEventMetadata} is not used.
         * @param eventType the event type
         * @since 1.2
         */
        public Builder<T> withEventType(final String eventType) {
            this.eventType = eventType;
            return this;
        }

        /**
         * Sets the attributes for the metadata if a {@link #withEventMetadata} is not used.
         * @param eventMetadataAttributes the attributes
         * @since 1.2
         */
        public Builder<T> withEventMetadataAttributes(final Map<String, Object> eventMetadataAttributes) {
            this.eventMetadataAttributes = eventMetadataAttributes;
            return this;
        }

        /**
         * Sets the time received for the metadata if a {@link #withEventMetadata} is not used.
         * @param timeReceived the time an event was received
         * @since 1.2
         */
        public Builder<T> withTimeReceived(final Instant timeReceived) {
            this.timeReceived = timeReceived;
            return this;
        }

        /**
         * Sets the metadata.
         * @param eventMetadata the metadata
         * @since 1.2
         */
        public Builder<T> withEventMetadata(final EventMetadata eventMetadata) {
            this.eventMetadata = eventMetadata;
            return this;
        }

        /**
         * Sets the data of the event.
         * @param data the data
         * @since 1.2
         */
        public Builder<T> withData(final Object data) {
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
