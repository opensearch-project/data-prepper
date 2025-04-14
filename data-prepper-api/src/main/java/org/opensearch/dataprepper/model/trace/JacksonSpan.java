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
import org.opensearch.dataprepper.model.validation.ParameterValidator;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jackson implementation for {@link Span}. This class extends the {@link JacksonEvent}.
 *
 * @since 1.2
 */
public class JacksonSpan extends JacksonEvent implements Span {

    private static final String STATUS_KEY = "status";
    private static final String SCOPE_KEY = "instrumentationScope";
    private static final String RESOURCE_KEY = "resource";
    private static final String TRACE_ID_KEY = "traceId";
    private static final String SPAN_ID_KEY = "spanId";
    private static final String TRACE_STATE_KEY = "traceState";
    private static final String PARENT_SPAN_ID_KEY = "parentSpanId";
    private static final String NAME_KEY = "name";
    private static final String FLAGS_KEY = "flags";
    private static final String KIND_KEY = "kind";
    private static final String START_TIME_KEY = "startTime";
    private static final String END_TIME_KEY = "endTime";
    protected static final String ATTRIBUTES_KEY = "attributes";
    private static final String DROPPED_ATTRIBUTES_COUNT_KEY = "droppedAttributesCount";
    private static final String EVENTS_KEY = "events";
    private static final String DROPPED_EVENTS_COUNT_KEY = "droppedEventsCount";
    private static final String LINKS_KEY = "links";
    private static final String DROPPED_LINKS_COUNT_KEY = "droppedLinksCount";
    public static final String SERVICE_NAME_KEY = "serviceName";
    public static final String TRACE_GROUP_KEY = "traceGroup";
    private static final String DURATION_IN_NANOS_KEY = "durationInNanos";
    public static final String TRACE_GROUP_FIELDS_KEY = "traceGroupFields";

    private static final List<String> REQUIRED_KEYS = Arrays.asList(TRACE_GROUP_KEY);
    protected static final List<String>
            REQUIRED_NON_EMPTY_KEYS = Arrays.asList(TRACE_ID_KEY, SPAN_ID_KEY, NAME_KEY, KIND_KEY, START_TIME_KEY, END_TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Arrays.asList(DURATION_IN_NANOS_KEY, TRACE_GROUP_FIELDS_KEY);

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };

    protected JacksonSpan(final Builder builder) {
        super(builder);
        validateParameters();
        checkAndSetDefaultValues();

        checkArgument(this.getMetadata().getEventType().equals("TRACE"), "eventType must be of type Trace");
    }

    private JacksonSpan(final JacksonSpan otherSpan) {
        super(otherSpan);
    }

    protected void validateParameters() {
        new ParameterValidator().validate(REQUIRED_KEYS, REQUIRED_NON_EMPTY_KEYS, REQUIRED_NON_NULL_KEYS, (HashMap<String, Object>)toMap());
    }

    protected void checkAndSetDefaultValues() {
        putIfAbsent(ATTRIBUTES_KEY, Map.class, new HashMap<>());
        putIfAbsent(DROPPED_ATTRIBUTES_COUNT_KEY, Integer.class, 0);
        putIfAbsent(LINKS_KEY, LinkedList.class, new LinkedList<>());
        putIfAbsent(DROPPED_LINKS_COUNT_KEY, Integer.class, 0);
        putIfAbsent(EVENTS_KEY, LinkedList.class, new LinkedList<>());
        putIfAbsent(DROPPED_EVENTS_COUNT_KEY, Integer.class, 0);
    }

    @Override
    public String getTraceId() {
        return this.get(TRACE_ID_KEY, String.class);
    }

    @Override
    public String getSpanId() {
        return this.get(SPAN_ID_KEY, String.class);
    }

    @Override
    public String getTraceState() {
        return this.get(TRACE_STATE_KEY, String.class);
    }

    @Override
    public String getParentSpanId() {
        return this.get(PARENT_SPAN_ID_KEY, String.class);
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
    public Integer getFlags() {
        return this.get(FLAGS_KEY, Integer.class);
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
    public String getStartTime() {
        return this.get(START_TIME_KEY, String.class);
    }

    @Override
    public String getEndTime() {
        return this.get(END_TIME_KEY, String.class);
    }

    @Override
    public Map<String, Object> getAttributes() {
        return this.get(ATTRIBUTES_KEY, Map.class);
    }

    @Override
    public Integer getDroppedAttributesCount() {
        return this.get(DROPPED_ATTRIBUTES_COUNT_KEY, Integer.class);
    }

    @Override
    public List<? extends SpanEvent> getEvents() {
        return this.getList(EVENTS_KEY, DefaultSpanEvent.class);
    }

    @Override
    public Integer getDroppedEventsCount() {
        return this.get(DROPPED_EVENTS_COUNT_KEY, Integer.class);
    }

    @Override
    public List<? extends Link> getLinks() {
        return this.getList(LINKS_KEY, DefaultLink.class);
    }

    @Override
    public Integer getDroppedLinksCount() {
        return this.get(DROPPED_LINKS_COUNT_KEY, Integer.class);
    }

    @Override
    public String getTraceGroup() {
        EventMetadata metadata = getMetadata();
        Object traceGroup = metadata.getAttribute(TRACE_GROUP_KEY);
        if (traceGroup != null)
            return (String)traceGroup;
        return this.get(TRACE_GROUP_KEY, String.class);
    }


    @Override
    public Long getDurationInNanos() {
        return this.get(DURATION_IN_NANOS_KEY, Long.class);
    }

    @Override
    public TraceGroupFields getTraceGroupFields() {
        EventMetadata metadata = getMetadata();
        Object traceGroupFields = metadata.getAttribute(TRACE_GROUP_FIELDS_KEY);
        if (traceGroupFields != null)
            return (TraceGroupFields)traceGroupFields;
        return this.get(TRACE_GROUP_FIELDS_KEY, DefaultTraceGroupFields.class);
    }

    @Override
    public String getServiceName() {
        EventMetadata metadata = getMetadata();
        Object serviceName = metadata.getAttribute(SERVICE_NAME_KEY);
        if (serviceName != null)
            return (String)serviceName;
        return this.get(SERVICE_NAME_KEY, String.class);
    }

    @Override
    public void setServiceName(final String serviceName) {
        this.put(SERVICE_NAME_KEY, serviceName);
    }

    @Override
    public void setTraceGroup(final String traceGroup) {
        this.put(TRACE_GROUP_KEY, traceGroup);
    }

    @Override
    public void setTraceGroupFields(final TraceGroupFields traceGroupFields) {
        this.put(TRACE_GROUP_FIELDS_KEY, traceGroupFields);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static JacksonSpan fromSpan(final Span span) {
        if (span instanceof JacksonSpan) {
            return new JacksonSpan((JacksonSpan) span);
        } else {
            return JacksonSpan.builder()
                    .withData(span.toMap())
                    .withEventMetadata(span.getMetadata())
                    .build();
        }
    }

    @Override
    public String toJsonString() {
        final ObjectNode attributesNode = (ObjectNode) getJsonNode().get("attributes");
        final ObjectNode flattenedJsonNode = getJsonNode().deepCopy();
        if (attributesNode != null) {
            flattenedJsonNode.remove("attributes");
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

        public Builder() {
            data = new HashMap();
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
            data.put(SPAN_ID_KEY, spanId);
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
            data.put(TRACE_ID_KEY, traceId);
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
            data.put(TRACE_STATE_KEY, traceState);
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
            data.put(PARENT_SPAN_ID_KEY, parentSpanId);
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
         * Sets the flags of span
         *
         * @param flags flags
         * @return returns the builder
         * @since 2.11
         */
        public Builder withFlags(final Integer flags) {
            data.put(FLAGS_KEY, flags);
            return this;
        }

        /**
         * Sets the status of the log event
         *
         * @param status status to be set
         * @return the builder
         * @since 2.11
         */
        public Builder withStatus(final Map<String, Object> status) {
            data.put(STATUS_KEY, status);
            return this;
        }

        /**
         * Sets the scope of the log event
         *
         * @param scope scope to be set
         * @return the builder
         * @since 2.11
         */
        public Builder withScope(final Map<String, Object> scope) {
            data.put(SCOPE_KEY, scope);
            return getThis();
        }

        /**
         * Sets the resource of the log event
         *
         * @param resource resource to be set
         * @return the builder
         * @since 2.11
         */
        public Builder withResource(final Map<String, Object> resource) {
            data.put(RESOURCE_KEY, resource);
            return getThis();
        }

        /**
         * Sets the start time of the span
         *
         * @param startTime start time
         * @return returns the builder
         * @since 1.2
         */
        public Builder withStartTime(final String startTime) {
            data.put(START_TIME_KEY, startTime);
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
            data.put(END_TIME_KEY, endTime);
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

        /**
         * Optional - sets the dropped attribute count for {@link JacksonSpan}. Default is 0.
         *
         * @param droppedAttributesCount the total number of dropped attributes
         * @return returns the builder
         * @since 1.2
         */
        public Builder withDroppedAttributesCount(final Integer droppedAttributesCount) {
            data.put(DROPPED_ATTRIBUTES_COUNT_KEY, droppedAttributesCount);
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
            data.put(DROPPED_EVENTS_COUNT_KEY, droppedEventsCount);
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
            data.put(DROPPED_LINKS_COUNT_KEY, droppedLinksCount);
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
            data.put(TRACE_GROUP_KEY, traceGroup);
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
            data.put(DURATION_IN_NANOS_KEY, durationInNanos);
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
            data.put(TRACE_GROUP_FIELDS_KEY, traceGroupFields);
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
            data.put(SERVICE_NAME_KEY, serviceName);
            return this;
        }

        protected void populateEvent() {
            super.withData(data);
            this.withEventType(EventType.TRACE.toString());
        }

        /**
         * Returns a newly created {@link JacksonSpan}
         *
         * @return a JacksonSpan
         * @since 1.2
         */
        @Override
        public JacksonSpan build() {
            populateEvent();
            return new JacksonSpan(this);
        }


    }
}
