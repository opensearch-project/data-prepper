/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Jackson Implementation of {@link Event} interface. This implementation relies heavily on JsonNode to manage the keys of the event.
 * <p>
 * This implementation supports [JsonPointer](https://datatracker.ietf.org/doc/html/rfc6901) for keys to access nested structures.
 * For example using the key "/fizz/buzz" would allow a user to retrieve the number 42 using {@link #get(String, Class)} from the nested structure below.
 * Additionally, a key structure without a prefixed "/" will access the same value: "fizz/buzz"
 * <p>
 * {
 * "foo": "bar"
 * "fizz": {
 * "buzz": 42
 * }
 * }
 *
 * @since 1.2
 */
public class JacksonEvent implements Event {

    private static final Logger LOG = LoggerFactory.getLogger(JacksonEvent.class);

    private static final String SEPARATOR = "/";

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .registerModule(new Jdk8Module()); // required for using Optional with Jackson. Ref: https://github.com/FasterXML/jackson-modules-java8

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {
    };

    private final EventMetadata eventMetadata;

    private EventHandle eventHandle;

    private final JsonNode jsonNode;

    static final int MAX_KEY_LENGTH = 2048;

    static final String MESSAGE_KEY = "message";

    static final String EVENT_TYPE = "event";

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

        this.jsonNode = getInitialJsonNode(builder.data);
    }

    protected JacksonEvent(final JacksonEvent otherEvent) {
        this.jsonNode = otherEvent.jsonNode.deepCopy();
        this.eventMetadata = DefaultEventMetadata.fromEventMetadata(otherEvent.eventMetadata);
    }

    public static Event fromMessage(String message) {
        return JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(Collections.singletonMap(MESSAGE_KEY, message))
                .build();
    }

    private JsonNode getInitialJsonNode(final Object data) {

        if (data == null) {
            return mapper.valueToTree(new HashMap<>());
        } else if (data instanceof String) {
            try {
                return mapper.readTree((String) data);
            } catch (final JsonProcessingException e) {
                throw new IllegalArgumentException("Unable to convert data into an event");
            }
        }
        return mapper.valueToTree(data);
    }

    protected JsonNode getJsonNode() {
        return jsonNode;
    }

    /**
     * Adds or updates the key with a given value in the Event.
     *
     * @param key   where the value will be set
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

    public void setEventHandle(EventHandle handle) {
        this.eventHandle = handle;
    }

    @Override
    public EventHandle getEventHandle() {
        return eventHandle;
    }

    private void setNode(final JsonNode parentNode, final String leafKey, final Object value) {
        final JsonNode valueNode = mapper.valueToTree(value);
        if (StringUtils.isNumeric(leafKey)) {
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
     *
     * @param key   the value to retrieve from
     * @param clazz the return type of the value
     * @return the value
     * @throws RuntimeException if it is unable to map the value to the provided clazz
     * @since 1.2
     */
    @Override
    public <T> T get(final String key, final Class<T> clazz) {

        final String trimmedKey = checkAndTrimKey(key);

        final JsonNode node = getNode(trimmedKey);
        if (node.isMissingNode()) {
            return null;
        }

        return mapNodeToObject(key, node, clazz);
    }

    private JsonNode getNode(final String key) {
        final JsonPointer jsonPointer = toJsonPointer(key);
        return jsonNode.at(jsonPointer);
    }

    private <T> T mapNodeToObject(final String key, final JsonNode node, final Class<T> clazz) {
        try {
            return mapper.treeToValue(node, clazz);
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to map {} to {}", key, clazz, e);
            throw new RuntimeException(String.format("Unable to map %s to %s", key, clazz), e);
        }
    }

    /**
     * Retrieves the given key from the Event as a List
     *
     * @param key   the value to retrieve from
     * @param clazz the return type of elements in the list
     * @return a List of clazz
     * @throws RuntimeException if it is unable to map the elements in the list to the provided clazz
     * @since 1.2
     */
    @Override
    public <T> List<T> getList(final String key, final Class<T> clazz) {

        final String trimmedKey = checkAndTrimKey(key);

        final JsonNode node = getNode(trimmedKey);
        if (node.isMissingNode()) {
            return null;
        }

        return mapNodeToList(key, node, clazz);
    }

    private <T> List<T> mapNodeToList(final String key, final JsonNode node, final Class<T> clazz) {
        try {
            final ObjectReader reader = mapper.readerFor(TypeFactory.defaultInstance().constructCollectionType(List.class, clazz));
            return reader.readValue(node);
        } catch (final IOException e) {
            LOG.error("Unable to map {} to List of {}", key, clazz, e);
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
    public String getAsJsonString(final String key) {
        final String trimmedKey = checkAndTrimKey(key);

        final JsonNode node = getNode(trimmedKey);
        if (node.isMissingNode()) {
            return null;
        }
        return node.toString();
    }

    /**
     * returns a string with formatted parts replaced by their values. The input
     * string may contain parts with format "${.../.../...}" which are replaced
     * by their value in the event
     *
     * @param format string with format
     * @throws RuntimeException if the format is incorrect or the value is not a string
     */
    @Override
    public String formatString(final String format) {
        int fromIndex = 0;
        String result = "";
        int position = 0;
        while ((position = format.indexOf("${", fromIndex)) != -1) {
            int endPosition = format.indexOf("}", position + 1);
            if (endPosition == -1) {
                throw new RuntimeException("Format string is not properly formed");
            }
            result += format.substring(fromIndex, position);
            String name = format.substring(position + 2, endPosition);
            Object val = this.get(name, Object.class);
            if (val == null) {
                return null;
            }
            result += val.toString();
            fromIndex = endPosition + 1;
        }
        if (fromIndex < format.length()) {
            result += format.substring(fromIndex);
        }
        return result;
    }

    @Override
    public EventMetadata getMetadata() {
        return eventMetadata;
    }

    @Override
    public boolean containsKey(final String key) {

        final String trimmedKey = checkAndTrimKey(key);

        final JsonNode node = getNode(trimmedKey);

        return !node.isMissingNode();
    }

    @Override
    public boolean isValueAList(final String key) {
        final String trimmedKey = checkAndTrimKey(key);

        final JsonNode node = getNode(trimmedKey);

        return node.isArray();
    }

    @Override
    public Map<String, Object> toMap() {
        return mapper.convertValue(jsonNode, MAP_TYPE_REFERENCE);
    }

    private String checkAndTrimKey(final String key) {
        checkKey(key);
        return trimKey(key);
    }

    private void checkKey(final String key) {
        checkNotNull(key, "key cannot be null");
        checkArgument(!key.isEmpty(), "key cannot be an empty string");
        if (key.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("key cannot be longer than " + MAX_KEY_LENGTH + " characters");
        }
        if (!isValidKey(key)) {
            throw new IllegalArgumentException("key " + key + " must contain only alphanumeric chars with .-_ and must follow JsonPointer (ie. 'field/to/key')");
        }
    }

    private String trimKey(final String key) {

        final String trimmedLeadingSlash = key.startsWith(SEPARATOR) ? key.substring(1) : key;
        return trimmedLeadingSlash.endsWith(SEPARATOR) ? trimmedLeadingSlash.substring(0, trimmedLeadingSlash.length() - 2) : trimmedLeadingSlash;
    }

    private boolean isValidKey(final String key) {
        char previous = ' ';
        char next = ' ';
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);

            if (i < key.length() - 1) {
                next = key.charAt(i + 1);
            }

            if ((i == 0 || i == key.length() - 1 || previous == '/' || next == '/') && (c == '_' || c == '.' || c == '-')) {
                return false;
            }

            if (!(c >= 48 && c <= 57
                    || c >= 65 && c <= 90
                    || c >= 97 && c <= 122
                    || c == '.'
                    || c == '-'
                    || c == '_'
                    || c == '@'
                    || c == '/')) {

                return false;
            }
            previous = c;
        }
        return true;
    }

    /**
     * Constructs an empty builder.
     *
     * @return a builder
     * @since 1.2
     */
    public static Builder builder() {
        return new Builder() {
            @Override
            public Builder getThis() {
                return this;
            }
        };
    }

    public static JsonStringBuilder jsonBuilder() {
        return new JsonStringBuilder();
    }

    public static JacksonEvent fromEvent(final Event event) {
        if (event instanceof JacksonEvent) {
            return new JacksonEvent((JacksonEvent) event);
        } else {
            return JacksonEvent.builder()
                    .withData(event.toMap())
                    .withEventMetadata(event.getMetadata())
                    .build();
        }
    }

    /**
     * Builder for creating {@link JacksonEvent}.
     *
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
         *
         * @param eventType the event type
         * @return returns the builder
         * @since 1.2
         */
        public Builder<T> withEventType(final String eventType) {
            this.eventType = eventType;
            return this;
        }

        /**
         * Sets the attributes for the metadata if a {@link #withEventMetadata} is not used.
         *
         * @param eventMetadataAttributes the attributes
         * @return returns the builder
         * @since 1.2
         */
        public Builder<T> withEventMetadataAttributes(final Map<String, Object> eventMetadataAttributes) {
            this.eventMetadataAttributes = eventMetadataAttributes;
            return this;
        }

        /**
         * Sets the time received for the metadata if a {@link #withEventMetadata} is not used.
         *
         * @param timeReceived the time an event was received
         * @return returns the builder
         * @since 1.2
         */
        public Builder<T> withTimeReceived(final Instant timeReceived) {
            this.timeReceived = timeReceived;
            return this;
        }

        /**
         * Sets the metadata.
         *
         * @param eventMetadata the metadata
         * @return returns the builder
         * @since 1.2
         */
        public Builder<T> withEventMetadata(final EventMetadata eventMetadata) {
            this.eventMetadata = eventMetadata;
            return this;
        }

        /**
         * Sets the data of the event.
         *
         * @param data the data
         * @return returns the builder
         * @since 1.2
         */
        public Builder<T> withData(final Object data) {
            this.data = data;
            return this;
        }

        /**
         * Returns a newly created {@link JacksonEvent}.
         *
         * @return an event
         * @since 1.2
         */
        public JacksonEvent build() {
            return new JacksonEvent(this);
        }
    }

    public static class JsonStringBuilder {
        private String tagsKey;
        private JacksonEvent event;

        public JsonStringBuilder withEvent(final JacksonEvent event) {
            this.event = event;
            return this;
        }

        public JsonStringBuilder includeTags(String key) {
            tagsKey = key;
            return this;
        }

        public String toJsonString() {
            if (event == null) {
                return null;
            }
            final String jsonString = event.toJsonString().trim();
            if(tagsKey != null) {
                final JsonNode tagsNode = mapper.valueToTree(event.getMetadata().getTags());
                return jsonString.substring(0, jsonString.length()-1) + ",\""+tagsKey+"\":" + tagsNode.toString()+"}";
            }
            return jsonString;
        }
    }
}
