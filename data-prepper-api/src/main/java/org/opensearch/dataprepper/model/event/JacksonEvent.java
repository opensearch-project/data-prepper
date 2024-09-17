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
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.exceptions.EventKeyNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.opensearch.dataprepper.model.event.JacksonEventKey.trimTrailingSlashInKey;

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

    private static final ObjectMapper mapper = JsonMapper.builder()
            .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
            .build()
            .registerModule(new JavaTimeModule())
            .registerModule(new Jdk8Module()); // required for using Optional with Jackson. Ref: https://github.com/FasterXML/jackson-modules-java8


    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };

    private final EventMetadata eventMetadata;

    private transient EventHandle eventHandle;

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
        if (builder.eventHandle != null) {
            this.eventHandle = builder.eventHandle;
        } else {
            this.eventHandle = new DefaultEventHandle(eventMetadata.getTimeReceived());
        }
        final Instant externalOriginationTime = this.eventMetadata.getExternalOriginationTime();
        if (externalOriginationTime != null) {
            eventHandle.setExternalOriginationTime(externalOriginationTime);
        }
    }

    protected JacksonEvent(final JacksonEvent otherEvent) {
        this.jsonNode = otherEvent.jsonNode.deepCopy();
        this.eventMetadata = DefaultEventMetadata.fromEventMetadata(otherEvent.eventMetadata);
        this.eventHandle = new DefaultEventHandle(eventMetadata.getTimeReceived());
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

    @Override
    public JsonNode getJsonNode() {
        return jsonNode;
    }

    @Override
    public void put(EventKey key, Object value) {
        final JacksonEventKey jacksonEventKey = asJacksonEventKey(key);

        if(!jacksonEventKey.supports(EventKeyFactory.EventAction.PUT)) {
            throw new IllegalArgumentException("key cannot be an empty string for put method");
        }

        final Deque<String> keys = new LinkedList<>(jacksonEventKey.getKeyPathList());

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

    /**
     * Adds or updates the key with a given value in the Event.
     *
     * @param key   where the value will be set
     * @param value value to set the key to
     * @since 1.2
     */
    @Override
    public void put(final String key, final Object value) {
        final JacksonEventKey jacksonEventKey = new JacksonEventKey(key, true, EventKeyFactory.EventAction.PUT);
        put(jacksonEventKey, value);
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

    @Override
    public <T> T get(EventKey key, Class<T> clazz) {
        JacksonEventKey jacksonEventKey = asJacksonEventKey(key);

        final JsonNode node = getNode(jacksonEventKey);
        if (node.isMissingNode()) {
            return null;
        }

        return mapNodeToObject(key.getKey(), node, clazz);
    }

    private static JacksonEventKey asJacksonEventKey(EventKey key) {
        if(!(key instanceof JacksonEventKey)) {
            throw new IllegalArgumentException("The key provided must be obtained through the EventKeyFactory.");
        }

        JacksonEventKey jacksonEventKey = (JacksonEventKey) key;
        return jacksonEventKey;
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
        final JacksonEventKey jacksonEventKey = new JacksonEventKey(key, true, EventKeyFactory.EventAction.GET);
        return get(jacksonEventKey, clazz);
    }

    private JsonNode getNode(final String key) {
        final JsonPointer jsonPointer = toJsonPointer(key);
        return jsonNode.at(jsonPointer);
    }

    private JsonNode getNode(final JacksonEventKey key) {
        return jsonNode.at(key.getJsonPointer());
    }

    private <T> T mapNodeToObject(final String key, final JsonNode node, final Class<T> clazz) {
        try {
            return mapper.treeToValue(node, clazz);
        } catch (final JsonProcessingException e) {
            LOG.error("Unable to map {} to {}", key, clazz, e);
            throw new RuntimeException(String.format("Unable to map %s to %s", key, clazz), e);
        }
    }

    @Override
    public <T> List<T> getList(EventKey key, Class<T> clazz) {
        JacksonEventKey jacksonEventKey = asJacksonEventKey(key);

        final JsonNode node = getNode(jacksonEventKey);
        if (node.isMissingNode()) {
            return null;
        }

        return mapNodeToList(jacksonEventKey.getKey(), node, clazz);
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
        JacksonEventKey jacksonEventKey = new JacksonEventKey(key, true, EventKeyFactory.EventAction.GET);
        return getList(jacksonEventKey, clazz);
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
        final String jsonPointerExpression;
        if (key.isEmpty() || key.startsWith("/")) {
            jsonPointerExpression = key;
        } else {
            jsonPointerExpression = SEPARATOR + key;
        }
        return JsonPointer.compile(jsonPointerExpression);
    }

    @Override
    public void delete(final EventKey key) {
        final JacksonEventKey jacksonEventKey = asJacksonEventKey(key);

        if(!jacksonEventKey.supports(EventKeyFactory.EventAction.DELETE)) {
            throw new IllegalArgumentException("key cannot be an empty string for delete method");
        }

        final String trimmedKey = jacksonEventKey.getTrimmedKey();
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

    /**
     * Deletes the key from the event.
     *
     * @param key the field to be deleted
     */
    @Override
    public void delete(final String key) {
        final JacksonEventKey jacksonEventKey = new JacksonEventKey(key, true, EventKeyFactory.EventAction.DELETE);
        delete(jacksonEventKey);
    }

    @Override
    public void clear() {
        // Delete all entries from the event
        Iterator iter = toMap().keySet().iterator();
        JsonNode baseNode = jsonNode;
        while (iter.hasNext()) {
            ((ObjectNode) baseNode).remove((String)iter.next());
        }
    }

    @Override
    public String toJsonString() {
        return jsonNode.toString();
    }

    @Override
    public String getAsJsonString(EventKey key) {

        JacksonEventKey jacksonEventKey = asJacksonEventKey(key);
        final JsonNode node = getNode(jacksonEventKey);
        if (node.isMissingNode()) {
            return null;
        }
        return node.toString();
    }

    @Override
    public String getAsJsonString(final String key) {
        JacksonEventKey jacksonEventKey = new JacksonEventKey(key, true, EventKeyFactory.EventAction.GET);
        return getAsJsonString(jacksonEventKey);
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
        return formatStringInternal(format, null, null);
    }

    /**
     * returns a string with formatted parts replaced by their values. The input
     * string may contain parts with format "${.../.../...}" which are replaced
     * by their value in the event. The input string may also contain Data Prepper expressions
     * such as "${getMetadata(\"some_metadata_key\")}
     *
     * @param format string with format
     * @throws RuntimeException if the format is incorrect or the value is not a string
     */
    @Override
    public String formatString(final String format, final ExpressionEvaluator expressionEvaluator) {
        return formatStringInternal(format, expressionEvaluator, null);
    }

    @Override
    public String formatString(final String format, final ExpressionEvaluator expressionEvaluator, final String defaultValue) {
        return formatStringInternal(format, expressionEvaluator, defaultValue);
    }


    private String formatStringInternal(final String format, final ExpressionEvaluator expressionEvaluator, final String defaultValue) {
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

            Object val = null;

            try {
                val = this.get(name, Object.class);
            } catch (final Exception ignored) {
                // Exception likely indicates use of a Data Prepper expression
            }

            if (val == null) {
                if (expressionEvaluator != null && expressionEvaluator.isValidExpressionStatement(name)) {
                    val = expressionEvaluator.evaluate(name, this);
                } else {
                    if (defaultValue == null) {
                        throw new EventKeyNotFoundException(String.format("The key %s could not be found in the Event when formatting", name));
                    }

                    val = defaultValue;
                }
            }


            if (Objects.nonNull(val)) {
                result += val.toString();
            }
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
    public boolean containsKey(EventKey key) {
        JacksonEventKey jacksonEventKey = asJacksonEventKey(key);

        final JsonNode node = getNode(jacksonEventKey);

        return !node.isMissingNode();
    }

    @Override
    public boolean containsKey(final String key) {
        JacksonEventKey jacksonEventKey = new JacksonEventKey(key, true, EventKeyFactory.EventAction.GET);
        return containsKey(jacksonEventKey);
    }

    @Override
    public boolean isValueAList(EventKey key) {
        JacksonEventKey jacksonEventKey = asJacksonEventKey(key);

        final JsonNode node = getNode(jacksonEventKey);

        return node.isArray();
    }

    @Override
    public boolean isValueAList(final String key) {
        JacksonEventKey jacksonEventKey = new JacksonEventKey(key, true, EventKeyFactory.EventAction.GET);
        return isValueAList(jacksonEventKey);
    }

    @Override
    public Map<String, Object> toMap() {
        return mapper.convertValue(jsonNode, MAP_TYPE_REFERENCE);
    }


    public static boolean isValidEventKey(final String key) {
        return JacksonEventKey.isValidEventKey(key);
    }

    private String trimKey(final String key) {

        final String trimmedLeadingSlash = key.startsWith(SEPARATOR) ? key.substring(1) : key;
        return trimTrailingSlashInKey(trimmedLeadingSlash);
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

    public JsonStringBuilder jsonBuilder() {
        return new JsonStringBuilder(this);
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
        protected Object data;
        private String eventType;
        private Instant timeReceived;
        private Map<String, Object> eventMetadataAttributes;
        protected EventHandle eventHandle;

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
         * Sets the event handle
         *
         * @param eventHandle event handle
         * @return returns the builder
         * @since 2.10
         */
        public Builder<T> withEventHandle(final EventHandle eventHandle) {
            this.eventHandle = eventHandle;
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

    public class JsonStringBuilder extends Event.JsonStringBuilder {

        private final boolean RETAIN_ALL = true;

        private final boolean EXCLUDE_ALL = false;


        private final JacksonEvent event;

        private JsonStringBuilder(final JacksonEvent event) {
            checkNotNull(event, "event cannot be null");
            this.event = event;
        }

        private JsonNode getBaseNode() {
            // Get root node.
            if (getRootKey() != null && !getRootKey().isEmpty() && event.containsKey(getRootKey())) {
                return event.getNode(getRootKey());
            }
            return event.getJsonNode();
        }


        public String toJsonString() {

            String jsonString;
            if (getIncludeKeys() != null && !getIncludeKeys().isEmpty()) {
                jsonString = searchAndFilter(getBaseNode(), "", getIncludeKeys(), RETAIN_ALL);
            } else if (getExcludeKeys() != null && !getExcludeKeys().isEmpty()) {
                jsonString = searchAndFilter(getBaseNode(), "", getExcludeKeys(), EXCLUDE_ALL);
            } else if (getBaseNode() !=event.getJsonNode()) {
                jsonString = event.getAsJsonString(getRootKey());
            } else {
                // Some successors have its own implementation of toJsonString, such as JacksonSpan.
                // In such case, it's only used when the root key is not provided.
                // TODO: Need to check if such behaviour is expected.
                jsonString = event.toJsonString();
            }

            final String tagsKey = getTagsKey();
            if (tagsKey != null) {
                final JsonNode tagsNode = mapper.valueToTree(event.getMetadata().getTags());
                return jsonString.substring(0, jsonString.length() - 1) + ",\"" + tagsKey + "\":" + tagsNode.toString() + "}";
            }
            return jsonString;
        }

        /**
         * Perform DFS(Depth-first search) like traversing using recursion on the Json Tree and return the json string.
         * This supports filtering (to include or exclude) from a list of keys.
         *
         * @param node         Root node to start traversing
         * @param path         Json path, e.g. /foo/bar
         * @param filterKeys   A list of filtered keys
         * @param filterAction Either to include (RETAIN_ALL or true) or to exclude (EXCLUDE_ALL or false)
         * @return a json string with filtered keys
         */
        String searchAndFilter(JsonNode node, String path, final List<String> filterKeys, boolean filterAction) {

            if (node.isArray()) { // for array node.
                StringJoiner sj = new StringJoiner(",", "[", "]");
                node.forEach(childNode -> sj.add(searchAndFilter(childNode, path, filterKeys, filterAction)));
                return sj.toString();
            } else {
                StringJoiner sj = new StringJoiner(",", "{", "}");
                List<String> valueList = new ArrayList<>();

                node.properties().forEach(entry -> {
                    String keyPath = trimKey(path + SEPARATOR + entry.getKey());
                    // Track whether the key is found in the filter list.
                    // Different behaviours between include and exclude action.
                    boolean found = false;
                    for (String key : filterKeys) {
                        key = trimKey(key);
                        if (keyPath.equals(key)) {
                            found = true;
                            // To keep the order.
                            if (filterAction == RETAIN_ALL) {
                                valueList.add("\"" + entry.getKey() + "\":" + entry.getValue().toString());
                            }
                            break;
                        } else if (key.startsWith(keyPath)) {
                            found = true;
                            valueList.add("\"" + entry.getKey() + "\":" + searchAndFilter(entry.getValue(), keyPath, filterKeys, filterAction));
                            break;
                        }
                        if (key.compareTo(keyPath) > 0) {
                            // To save the comparing.
                            // This requires the filter keys to be sorted first.
                            // This is done in SinkModel.
                            break;
                        }
                    }

                    if (!found && filterAction == EXCLUDE_ALL) {
                        valueList.add("\"" + entry.getKey() + "\":" + entry.getValue().toString());
                    }
                });

                valueList.forEach(value -> sj.add(value));
                return sj.toString();

            }
        }
    }

    /**
     * Provides custom Java object deserialization.
     *
     * @param objectInputStream The {@link ObjectInputStream} to deserialize from
     * @throws IOException if an I/O error occurs
     * @throws ClassNotFoundException if the class of a serialized object could not be found
     */
    private void readObject(final ObjectInputStream objectInputStream) throws IOException, ClassNotFoundException {
        objectInputStream.defaultReadObject();
        this.eventHandle = new DefaultEventHandle(eventMetadata.getTimeReceived());
    }
}
