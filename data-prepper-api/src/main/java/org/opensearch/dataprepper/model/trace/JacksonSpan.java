/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.trace;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A Jackson implementation for {@link Span}. This class extends the {@link JacksonEvent}.
 *
 * @since 1.2
 */
public class JacksonSpan extends JacksonEvent implements Span {

    private static final String TRACE_ID_KEY = "traceId";
    private static final String OTLP_TRACE_ID_KEY = "trace_id";
    private static final String SPAN_ID_KEY = "spanId";
    private static final String OTLP_SPAN_ID_KEY = "span_id";
    private static final String TRACE_STATE_KEY = "traceState";
    private static final String OTLP_TRACE_STATE_KEY = "trace_state";
    private static final String PARENT_SPAN_ID_KEY = "parentSpanId";
    private static final String OTLP_PARENT_SPAN_ID_KEY = "parent_span_id";
    private static final String NAME_KEY = "name";
    private static final String KIND_KEY = "kind";
    private static final String STATUS_KEY = "status";
    private static final String SCOPE_KEY = "scope";
    private static final String RESOURCE_KEY = "resource";
    private static final String START_TIME_KEY = "startTime";
    private static final String OTLP_START_TIME_KEY = "start_time";
    private static final String END_TIME_KEY = "endTime";
    private static final String OTLP_END_TIME_KEY = "end_time";
    private static final String ATTRIBUTES_KEY = "attributes";
    private static final String DROPPED_ATTRIBUTES_COUNT_KEY = "droppedAttributesCount";
    private static final String OTLP_DROPPED_ATTRIBUTES_COUNT_KEY = "dropped_attributes_count";
    private static final String EVENTS_KEY = "events";
    private static final String DROPPED_EVENTS_COUNT_KEY = "droppedEventsCount";
    private static final String OTLP_DROPPED_EVENTS_COUNT_KEY = "dropped_events_count";
    private static final String LINKS_KEY = "links";
    private static final String DROPPED_LINKS_COUNT_KEY = "droppedLinksCount";
    private static final String OTLP_DROPPED_LINKS_COUNT_KEY = "dropped_links_count";
    private static final String SERVICE_NAME_KEY = "serviceName";
    private static final String OTLP_SERVICE_NAME_KEY = "service_name";
    private static final String TRACE_GROUP_KEY = "traceGroup";
    private static final String OTLP_TRACE_GROUP_KEY = "trace_group";
    private static final String DURATION_IN_NANOS_KEY = "durationInNanos";
    private static final String OTLP_DURATION_IN_NANOS_KEY = "duration_in_nanos";
    private static final String TRACE_GROUP_FIELDS_KEY = "traceGroupFields";
    private static final String OTLP_TRACE_GROUP_FIELDS_KEY = "trace_group_fields";

    private static final List<String> REQUIRED_KEYS = Arrays.asList(TRACE_GROUP_KEY);
    private static final List<String>
            REQUIRED_NON_EMPTY_KEYS = Arrays.asList(TRACE_ID_KEY, SPAN_ID_KEY, NAME_KEY, KIND_KEY, START_TIME_KEY, END_TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Arrays.asList(DURATION_IN_NANOS_KEY, TRACE_GROUP_FIELDS_KEY);

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };
    private boolean opensearchMode;

    protected JacksonSpan(final Builder builder, boolean opensearchMode) {
        super(builder);
        this.opensearchMode = opensearchMode;

        checkArgument(this.getMetadata().getEventType().equals("TRACE"), "eventType must be of type Trace");
    }

    private JacksonSpan(final JacksonSpan otherSpan) {
        super(otherSpan);
    }

    public void setOpensearchMode(final boolean opensearchMode) {
        this.opensearchMode = opensearchMode;
    }

    @Override
    public boolean getOpensearchMode() {
        return opensearchMode;
    }

    @Override
    public String getTraceId() {
        final String key = (opensearchMode) ? TRACE_ID_KEY : OTLP_TRACE_ID_KEY;
        return this.get(key, String.class);
    }

    @Override
    public String getSpanId() {
        final String key = (opensearchMode) ? SPAN_ID_KEY : OTLP_SPAN_ID_KEY;
        return this.get(key, String.class);
    }

    @Override
    public String getTraceState() {
        final String key = (opensearchMode) ? TRACE_STATE_KEY : OTLP_TRACE_STATE_KEY;
        return this.get(key, String.class);
    }

    @Override
    public String getParentSpanId() {
        final String key = (opensearchMode) ? PARENT_SPAN_ID_KEY : OTLP_PARENT_SPAN_ID_KEY;
        return this.get(key, String.class);
    }

    @Override
    public String getName() {
        return this.get(NAME_KEY, String.class);
    }

    @Override
    public String getKind() {
        return this.get(KIND_KEY, String.class);
    }

    @Override
    public String getStartTime() {
        final String key = (opensearchMode) ? START_TIME_KEY : OTLP_START_TIME_KEY;
        return this.get(key, String.class);
    }

    @Override
    public String getEndTime() {
        final String key = (opensearchMode) ? END_TIME_KEY : OTLP_END_TIME_KEY;
        return this.get(key, String.class);
    }

    @Override
    public Map<String, Object> getAttributes() {
        return this.get(ATTRIBUTES_KEY, Map.class);
    }

    @Override
    public Map<String, Object> getScope() {
        return this.get(SCOPE_KEY, Map.class);
    }

    @Override
    public Map<String, Object> getResource() {
        return this.get(RESOURCE_KEY, Map.class);
    }

    @Override
    public Map<String, Object> getStatus() {
        return this.get(STATUS_KEY, Map.class);
    }

    @Override
    public Integer getDroppedAttributesCount() {
        final String key = (opensearchMode) ? DROPPED_ATTRIBUTES_COUNT_KEY : OTLP_DROPPED_ATTRIBUTES_COUNT_KEY;
        return this.get(key, Integer.class);
    }

    @Override
    public List<? extends SpanEvent> getEvents() {
        return this.getList(EVENTS_KEY, DefaultSpanEvent.class);
    }

    @Override
    public Integer getDroppedEventsCount() {
        final String key = (opensearchMode) ? DROPPED_EVENTS_COUNT_KEY : OTLP_DROPPED_EVENTS_COUNT_KEY;
        return this.get(key, Integer.class);
    }

    @Override
    public List<? extends Link> getLinks() {
        return this.getList(LINKS_KEY, DefaultLink.class);
    }

    @Override
    public Integer getDroppedLinksCount() {
        final String key = (opensearchMode) ? DROPPED_LINKS_COUNT_KEY : OTLP_DROPPED_LINKS_COUNT_KEY;
        return this.get(key, Integer.class);
    }

    @Override
    public String getTraceGroup() {
        final String key = (opensearchMode) ? TRACE_GROUP_KEY : OTLP_TRACE_GROUP_KEY;
        return this.get(key, String.class);
    }

    @Override
    public Long getDurationInNanos() {
        final String key = (opensearchMode) ? DURATION_IN_NANOS_KEY : OTLP_DURATION_IN_NANOS_KEY;
        return this.get(key, Long.class);
    }

    @Override
    public TraceGroupFields getTraceGroupFields() {
        final String key = (opensearchMode) ? TRACE_GROUP_FIELDS_KEY : OTLP_TRACE_GROUP_FIELDS_KEY;
        return this.get(key, DefaultTraceGroupFields.class);
    }

    @Override
    public String getServiceName() {
        final String key = (opensearchMode) ? SERVICE_NAME_KEY : OTLP_SERVICE_NAME_KEY;
        return this.get(key, String.class);
    }

    @Override
    public void setTraceGroup(final String traceGroup) {
        final String key = (opensearchMode) ? TRACE_GROUP_KEY : OTLP_TRACE_GROUP_KEY;
        this.put(key, traceGroup);
    }

    @Override
    public void setTraceGroupFields(final TraceGroupFields traceGroupFields) {
        final String key = (opensearchMode) ? TRACE_GROUP_FIELDS_KEY : OTLP_TRACE_GROUP_FIELDS_KEY;
        this.put(key, traceGroupFields);
    }

    public static Builder builder(final boolean opensearchMode) {
        return new Builder(opensearchMode);
    }

    public static JacksonSpan fromSpan(final Span span) {
        if (span instanceof JacksonSpan) {
            return new JacksonSpan((JacksonSpan) span);
        } else {
            return JacksonSpan.builder(span.getOpensearchMode())
                    .withData(span.toMap())
                    .withEventMetadata(span.getMetadata())
                    .build();
        }
    }

    @Override
    public String toJsonString() {
        if (!opensearchMode) {
            return getJsonNode().toString();
        }
        final ObjectNode attributesNode = (ObjectNode) getJsonNode().get(ATTRIBUTES_KEY);
        final ObjectNode flattenedJsonNode = getJsonNode().deepCopy();
        if (attributesNode != null) {
            flattenedJsonNode.remove(ATTRIBUTES_KEY);
            for (Iterator<Map.Entry<String, JsonNode>> it = attributesNode.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                String field = entry.getKey();
                if (!flattenedJsonNode.has(field)) {
                    flattenedJsonNode.set(field, entry.getValue());
                }
            }
        }
        return flattenedJsonNode.toString();
    }

    /**
     * Builder for creating {@link JacksonSpan}
     *
     * @since 1.2
     */
    public static class Builder extends JacksonEvent.Builder<Builder> {

        private final Map<String, Object> data;
        private boolean opensearchMode;

        public Builder(final boolean opensearchMode) {
            data = new HashMap();
            this.opensearchMode = opensearchMode;
        }

        @Override
        public Builder getThis() {
            return this;
        }

        /**
         * Sets the data of the event.
         *
         * @param data JSON representation of the data
         * @return returns the builder
         * @since 2.0
         */
        public Builder withJsonData(final String data) {
            try {
                this.data.putAll(mapper.readValue(data, MAP_TYPE_REFERENCE));
            } catch (final JsonProcessingException e) {
                throw new RuntimeException("An exception occurred due to invalid JSON while converting data to event");
            }
            return this;
        }

        /**
         * Sets the data of the event.
         *
         * @param data the data
         * @return returns the builder
         * @since 2.0
         */
        @Override
        public Builder withData(final Object data) {
            this.data.putAll(mapper.convertValue(data, Map.class));
            return this;
        }

        /**
         * Sets the metadata.
         *
         * @param eventMetadata the metadata
         * @return returns the builder
         * @since 2.0
         */
        @Override
        public Builder withEventMetadata(final EventMetadata eventMetadata) {
            super.withEventMetadata(eventMetadata);
            return this;
        }

        /**
         * Sets the span id.
         *
         * @param spanId span id
         * @return returns the builder
         * @since 1.2
         */
        public Builder withSpanId(final String spanId) {
            final String key = (opensearchMode) ? SPAN_ID_KEY : OTLP_SPAN_ID_KEY;
            data.put(key, spanId);
            return this;
        }

        /**
         * Sets the trace id for the span.
         *
         * @param traceId trace id
         * @return returns the builder
         * @since 1.2
         */
        public Builder withTraceId(final String traceId) {
            final String key = (opensearchMode) ? TRACE_ID_KEY : OTLP_TRACE_ID_KEY;
            data.put(key, traceId);
            return this;
        }

        /**
         * Sets the trace state
         *
         * @param traceState trace state
         * @return returns the builder
         * @since 1.2
         */
        public Builder withTraceState(final String traceState) {
            final String key = (opensearchMode) ? TRACE_STATE_KEY : OTLP_TRACE_STATE_KEY;
            data.put(key, traceState);
            return this;
        }

        /**
         * Sets the parent span id.
         *
         * @param parentSpanId parent span id
         * @return returns the builder
         * @since 1.2
         */
        public Builder withParentSpanId(final String parentSpanId) {
            final String key = (opensearchMode) ? PARENT_SPAN_ID_KEY : OTLP_PARENT_SPAN_ID_KEY;
            data.put(key, parentSpanId);
            return this;
        }

        /**
         * Sets the span name
         *
         * @param name name
         * @return returns the builder
         * @since 1.2
         */
        public Builder withName(final String name) {
            data.put(NAME_KEY, name);
            return this;
        }

        /**
         * Sets the type of span
         *
         * @param kind kind
         * @return returns the builder
         * @since 1.2
         */
        public Builder withKind(final String kind) {
            data.put(KIND_KEY, kind);
            return this;
        }

        /**
         * Sets the start time of the span
         *
         * @param startTime start time
         * @return returns the builder
         * @since 1.2
         */
        public Builder withStartTime(final String startTime) {
            final String key = (opensearchMode) ? START_TIME_KEY : OTLP_START_TIME_KEY;
            data.put(key, startTime);
            return this;
        }

        /**
         * Sets the end time of the span
         *
         * @param endTime end time
         * @return returns the builder
         * @since 1.2
         */
        public Builder withEndTime(final String endTime) {
            final String key = (opensearchMode) ? END_TIME_KEY : OTLP_END_TIME_KEY;
            data.put(key, endTime);
            return this;
        }

        /**
         * Optional - sets the attributes for {@link JacksonSpan}. Default is an empty map.
         *
         * @param attributes the attributes to associate with this event.
         * @return returns the builder
         * @since 1.2
         */
        public Builder withAttributes(final Map<String, Object> attributes) {
            data.put(ATTRIBUTES_KEY, attributes);
            return this;
        }

        public Builder withScope(final Map<String, Object> scope) {
            data.put(SCOPE_KEY, scope);
            return this;
        }

        public Builder withResource(final Map<String, Object> resource) {
            data.put(RESOURCE_KEY, resource);
            return this;
        }

        public Builder withStatus(final Map<String, Object> status) {
            data.put(STATUS_KEY, status);
            return this;
        }

        /**
         * Optional - sets the dropped attribute count for {@link JacksonSpan}. Default is 0.
         *
         * @param droppedAttributesCount the total number of dropped attributes
         * @return returns the builder
         * @since 1.2
         */
        public Builder withDroppedAttributesCount(final Integer droppedAttributesCount) {
            final String key = (opensearchMode) ? DROPPED_ATTRIBUTES_COUNT_KEY : OTLP_DROPPED_ATTRIBUTES_COUNT_KEY;
            data.put(key, droppedAttributesCount);
            return this;
        }

        /**
         * Optional - sets the events for {@link JacksonSpan}. Default is an empty list.
         *
         * @param events the events to associate.
         * @return returns the builder
         * @since 1.2
         */
        public Builder withEvents(final List<? extends SpanEvent> events) {
            data.put(EVENTS_KEY, events);
            return this;
        }

        /**
         * Optional - sets the dropped events count for {@link JacksonSpan}. Default is 0.
         *
         * @param droppedEventsCount the total number of dropped events
         * @return returns the builder
         * @since 1.2
         */
        public Builder withDroppedEventsCount(final Integer droppedEventsCount) {
            final String key = (opensearchMode) ? DROPPED_EVENTS_COUNT_KEY : OTLP_DROPPED_EVENTS_COUNT_KEY;
            data.put(key, droppedEventsCount);
            return this;
        }

        /**
         * Optional - sets the links for {@link JacksonSpan}. Default is an empty list.
         *
         * @param links the links to associate.
         * @return returns the builder
         * @since 1.2
         */
        public Builder withLinks(final List<? extends Link> links) {
            data.put(LINKS_KEY, links);
            return this;
        }

        /**
         * Optional - sets the dropped links count for {@link JacksonSpan}. Default is 0.
         *
         * @param droppedLinksCount the total number of dropped links
         * @return returns the builder
         * @since 1.2
         */
        public Builder withDroppedLinksCount(final Integer droppedLinksCount) {
        final String key = (opensearchMode) ? DROPPED_LINKS_COUNT_KEY : OTLP_DROPPED_LINKS_COUNT_KEY;
            data.put(key, droppedLinksCount);
            return this;
        }

        /**
         * Sets the trace group name
         *
         * @param traceGroup trace group
         * @return returns the builder
         * @since 1.2
         */
        public Builder withTraceGroup(final String traceGroup) {
            final String key = (opensearchMode) ? TRACE_GROUP_KEY : OTLP_TRACE_GROUP_KEY;
            data.put(key, traceGroup);
            return this;
        }

        /**
         * Sets the time received for populating event origination time in event handle
         *
         * @param timeReceived time received
         * @return the builder
         * @since 2.7
         */
        @Override
        public Builder withTimeReceived(final Instant timeReceived) {
            return (Builder)super.withTimeReceived(timeReceived);
        }

        /**
         * Sets the duration of the span
         *
         * @param durationInNanos duration of the span in nano seconds
         * @return returns the builder
         * @since 1.2
         */
        public Builder withDurationInNanos(final Long durationInNanos) {
            final String key = (opensearchMode) ? DURATION_IN_NANOS_KEY : OTLP_DURATION_IN_NANOS_KEY;
            data.put(key, durationInNanos);
            return this;
        }

        /**
         * Sets the trace group fields
         *
         * @param traceGroupFields trace group fields
         * @return returns the builder
         * @since 1.2
         */
        public Builder withTraceGroupFields(final TraceGroupFields traceGroupFields) {
            final String key = (opensearchMode) ? TRACE_GROUP_FIELDS_KEY : OTLP_TRACE_GROUP_FIELDS_KEY;
            data.put(key, traceGroupFields);
            return this;
        }

        /**
         * Sets the service name of the span
         *
         * @param serviceName name of the service
         * @return returns the builder
         * @since 1.3
         */
        public Builder withServiceName(final String serviceName) {
            final String key = (opensearchMode) ? SERVICE_NAME_KEY : OTLP_SERVICE_NAME_KEY;
            data.put(key, serviceName);
            return this;
        }

        /**
         * Returns a newly created {@link JacksonSpan}
         *
         * @return a JacksonSpan
         * @since 1.2
         */

        @Override
        public JacksonSpan build() {
            validateParameters();
            checkAndSetDefaultValues();
            super.withData(data);
            this.withEventType(EventType.TRACE.toString());
            return new JacksonSpan(this, opensearchMode);
        }

        private void validateParameters() {
            if (opensearchMode) {
                REQUIRED_KEYS.forEach(key -> {
                    checkState(data.containsKey(key), key + " need to be assigned");
                });

                REQUIRED_NON_EMPTY_KEYS.forEach(key -> {
                    final String value = (String) data.get(key);
                    checkNotNull(value, key + " cannot be null");
                    checkArgument(!value.isEmpty(), key + " cannot be an empty string");
                });

                REQUIRED_NON_NULL_KEYS.forEach(key -> {
                    final Object value = data.get(key);
                    checkNotNull(value, key + " cannot be null");
                });
            }
        }

        private void checkAndSetDefaultValues() {
            if (opensearchMode) {
                data.computeIfAbsent(ATTRIBUTES_KEY, k -> new HashMap<>());
                data.putIfAbsent(DROPPED_ATTRIBUTES_COUNT_KEY, 0);
                data.computeIfAbsent(LINKS_KEY, k -> new LinkedList<>());
                data.putIfAbsent(DROPPED_LINKS_COUNT_KEY, 0);
                data.computeIfAbsent(EVENTS_KEY, k -> new LinkedList<>());
                data.putIfAbsent(DROPPED_EVENTS_COUNT_KEY, 0);
            }
        }

    }
}
