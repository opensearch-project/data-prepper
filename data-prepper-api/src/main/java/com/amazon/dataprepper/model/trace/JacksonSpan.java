package com.amazon.dataprepper.model.trace;

import com.amazon.dataprepper.model.event.EventType;
import com.amazon.dataprepper.model.event.JacksonEvent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Jackson implementation for {@link Span}. This class extends the {@link JacksonEvent}.
 *
 * @since 1.2
 */
public class JacksonSpan extends JacksonEvent implements Span {

    private static final String TRACE_ID_KEY = "traceId";
    private static final String SPAN_ID_KEY = "spanId";
    private static final String TRACE_STATE_KEY = "traceState";
    private static final String PARENT_SPAN_ID_KEY = "parentSpanId";
    private static final String NAME_KEY = "name";
    private static final String KIND_KEY = "kind";
    private static final String START_TIME_KEY = "startTime";
    private static final String END_TIME_KEY = "endTime";
    private static final String ATTRIBUTES_KEY = "attributes";
    private static final String DROPPED_ATTRIBUTES_COUNT_KEY = "droppedAttributesCount";
    private static final String EVENTS_KEY = "events";
    private static final String DROPPED_EVENTS_COUNT_KEY = "droppedEventsCount";
    private static final String LINKS_KEY = "links";
    private static final String DROPPED_LINKS_COUNT_KEY = "droppedLinksCount";
    private static final String TRACE_GROUP_KEY = "traceGroup";
    private static final String DURATION_IN_NANOS_KEY = "durationInNanos";
    private static final String TRACE_GROUP_FIELDS_KEY = "traceGroupFields";

    private static final List<String> ALL_KEYS = Arrays.asList(TRACE_ID_KEY, SPAN_ID_KEY, TRACE_STATE_KEY, PARENT_SPAN_ID_KEY, PARENT_SPAN_ID_KEY,
            NAME_KEY, KIND_KEY, START_TIME_KEY, END_TIME_KEY, TRACE_GROUP_KEY, DURATION_IN_NANOS_KEY, TRACE_GROUP_FIELDS_KEY);

    protected JacksonSpan(final Builder builder) {
        super(builder);

        checkArgument(this.getMetadata().getEventType().equals("TRACE"), "eventType must be of type Trace");

        checkAndSetDefaultValues();
    }

    @Override public String getTraceId() {
        return this.get(TRACE_ID_KEY, String.class);
    }

    @Override public String getSpanId() {
        return this.get(SPAN_ID_KEY, String.class);
    }

    @Override public String getTraceState() {
        return this.get(TRACE_STATE_KEY, String.class);
    }

    @Override public String getParentSpanId() {
        return this.get(PARENT_SPAN_ID_KEY, String.class);
    }

    @Override public String getName() {
        return this.get(NAME_KEY, String.class);
    }

    @Override public String getKind() {
        return this.get(KIND_KEY, String.class);
    }

    @Override public String getStartTime() {
        return this.get(START_TIME_KEY, String.class);
    }

    @Override public String getEndTime() {
        return this.get(END_TIME_KEY, String.class);
    }

    @Override public Map<String, Object> getAttributes() {
        return this.get(ATTRIBUTES_KEY, Map.class);
    }

    @Override public Integer getDroppedAttributesCount() {
        return this.get(DROPPED_ATTRIBUTES_COUNT_KEY, Integer.class);
    }

    @Override public List<? extends SpanEvent> getEvents() {
        return this.getList(EVENTS_KEY, DefaultSpanEvent.class);
    }

    @Override public Integer getDroppedEventsCount() {
        return this.get(DROPPED_EVENTS_COUNT_KEY, Integer.class);
    }

    @Override public List<? extends Link> getLinks() {
        return this.getList(LINKS_KEY, DefaultLink.class);
    }

    @Override public Integer getDroppedLinksCount() {
        return this.get(DROPPED_LINKS_COUNT_KEY, Integer.class);
    }

    @Override public String getTraceGroup() {
        return this.get(TRACE_GROUP_KEY, String.class);
    }

    @Override public Long getDurationInNanos() {
        return this.get(DURATION_IN_NANOS_KEY, Long.class);
    }

    @Override public TraceGroupFields getTraceGroupFields() {
        return this.get(TRACE_GROUP_FIELDS_KEY, DefaultTraceGroupFields.class);
    }

    private void checkAndSetDefaultValues() {
        if (this.getAttributes() == null ) {
            this.put(ATTRIBUTES_KEY, new HashMap<>());
        }

        if (this.getDroppedAttributesCount() == null) {
            this.put(DROPPED_ATTRIBUTES_COUNT_KEY, 0);
        }

        if (this.getLinks() == null) {
            this.put(LINKS_KEY, new LinkedList<>());
        }

        if (this.getDroppedLinksCount() == null) {
            this.put(DROPPED_LINKS_COUNT_KEY, 0);
        }

        if (this.getEvents() == null) {
            this.put(EVENTS_KEY, new LinkedList<>());
        }

        if (this.getDroppedEventsCount() == null) {
            this.put(DROPPED_EVENTS_COUNT_KEY, 0);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating {@link JacksonSpan}
     * @since 1.2
     */
    public static class Builder  extends JacksonEvent.Builder<Builder> {

        private Map<String, Object> data;

        private Set<String> remainingRequiredFields;

        public Builder() {
            data = new HashMap();
            remainingRequiredFields = new HashSet(ALL_KEYS);
        }

        @Override public Builder getThis() {
            return this;
        }

        /**
         * Sets the span id.
         * @param spanId
         * @since 1.2
         */
        public Builder withSpanId(final String spanId) {
            checkNotNull(spanId, "spanId cannot be null");
            checkArgument(!spanId.isEmpty(), "spanId cannot be an empty string");
            putAndMarkAsProvided(SPAN_ID_KEY, spanId);
            return this;
        }

        /**
         * Sets the trace id for the span.
         * @param traceId
         * @since 1.2
         */
        public Builder withTraceId(final String traceId) {
            checkNotNull(traceId, "traceId cannot be null");
            checkArgument(!traceId.isEmpty(), "traceId cannot be an empty string");
            putAndMarkAsProvided(TRACE_ID_KEY, traceId);
            return this;
        }

        /**
         * Sets the trace state
         * @param traceState
         * @since 1.2
         */
        public Builder withTraceState(final String traceState) {
            checkNotNull(traceState, "traceState cannot be null");
            checkArgument(!traceState.isEmpty(), "traceState cannot be an empty string");
            putAndMarkAsProvided(TRACE_STATE_KEY, traceState);
            return this;
        }

        /**
         * Sets the parent span id.
         * @param parentSpanId
         * @since 1.2
         */
        public Builder withParentSpanId(final String parentSpanId) {
            checkNotNull(parentSpanId, "parentSpanId cannot be null");
            checkArgument(!parentSpanId.isEmpty(), "parentSpanId cannot be an empty string");
            putAndMarkAsProvided(PARENT_SPAN_ID_KEY, parentSpanId);
            return this;
        }

        /**
         * Sets the span name
         * @param name
         * @since 1.2
         */
        public Builder withName(final String name) {
            checkNotNull(name, "name cannot be null");
            checkArgument(!name.isEmpty(), "name cannot be an empty string");
            putAndMarkAsProvided(NAME_KEY, name);
            return this;
        }

        /**
         * Sets the type of span
         * @param kind
         * @since 1.2
         */
        public Builder withKind(final String kind) {
            checkNotNull(kind, "kind cannot be null");
            checkArgument(!kind.isEmpty(), "kind cannot be an empty string");
            putAndMarkAsProvided(KIND_KEY, kind);
            return this;
        }

        /**
         * Sets the start time of the span
         * @param startTime
         * @since 1.2
         */
        public Builder withStartTime(final String startTime) {
            checkNotNull(startTime, "startTime cannot be null");
            checkArgument(!startTime.isEmpty(), "startTime cannot be an empty string");
            putAndMarkAsProvided(START_TIME_KEY, startTime);
            return this;
        }

        /**
         * Sets the end time of the span
         * @param endTime
         * @since 1.2
         */
        public Builder withEndTime(final String endTime) {
            checkNotNull(endTime, "endTime cannot be null");
            checkArgument(!endTime.isEmpty(), "endTime cannot be an empty string");
            putAndMarkAsProvided(END_TIME_KEY, endTime);
            return this;
        }

        /**
         * Optional - sets the attributes for {@link JacksonSpan}. Default is an empty map.
         * @param attributes the attributes to associate with this event.
         * @since 1.2
         */
        public Builder withAttributes(final Map<String, Object> attributes) {
            checkNotNull(attributes, "attributes cannot be null");
            data.put(ATTRIBUTES_KEY, attributes);
            return this;
        }

        /**
         * Optional - sets the dropped attribute count for {@link JacksonSpan}. Default is 0.
         * @param droppedAttributesCount the total number of dropped attributes
         * @since 1.2
         */
        public Builder withDroppedAttributesCount(final Integer droppedAttributesCount) {
            checkNotNull(droppedAttributesCount, "droppedAttributesCount cannot be null");
            data.put(DROPPED_ATTRIBUTES_COUNT_KEY, droppedAttributesCount);
            return this;
        }

        /**
         * Optional - sets the events for {@link JacksonSpan}. Default is an empty list.
         * @param events the events to associate.
         * @since 1.2
         */
        public Builder withEvents(final List<SpanEvent> events) {
            checkNotNull(events, "events cannot be null");
            data.put(EVENTS_KEY, events);
            return this;
        }

        /**
         * Optional - sets the dropped events count for {@link JacksonSpan}. Default is 0.
         * @param droppedEventsCount the total number of dropped events
         * @since 1.2
         */
        public Builder withDroppedEventsCount(final Integer droppedEventsCount) {
            checkNotNull(droppedEventsCount, "droppedEventsCount cannot be null");
            data.put(DROPPED_EVENTS_COUNT_KEY, droppedEventsCount);
            return this;
        }

        /**
         * Optional - sets the links for {@link JacksonSpan}. Default is an empty list.
         * @param links the links to associate.
         * @since 1.2
         */
        public Builder withLinks(final List<Link> links) {
            checkNotNull(links, "links cannot be null");
            data.put(LINKS_KEY, links);
            return this;
        }

        /**
         * Optional - sets the dropped links count for {@link JacksonSpan}. Default is 0.
         * @param droppedLinksCount the total number of dropped links
         * @since 1.2
         */
        public Builder withDroppedLinksCount(final Integer droppedLinksCount) {
            checkNotNull(droppedLinksCount, "droppedLinksCount cannot be null");
            data.put(DROPPED_LINKS_COUNT_KEY, droppedLinksCount);
            return this;
        }

        /**
         * Sets the trace group name
         * @param traceGroup
         * @since 1.2
         */
        public Builder withTraceGroup(final String traceGroup) {
            checkNotNull(traceGroup, "traceGroup cannot be null");
            checkArgument(!traceGroup.isEmpty(), "traceGroup cannot be an empty string");
            putAndMarkAsProvided(TRACE_GROUP_KEY, traceGroup);
            return this;
        }

        /**
         * Sets the duration of the span
         * @param durationInNanos
         * @since 1.2
         */
        public Builder withDurationInNanos(final Long durationInNanos) {
            checkNotNull(durationInNanos, "durationInNanos cannot be null");
            putAndMarkAsProvided(DURATION_IN_NANOS_KEY, durationInNanos);
            return this;
        }

        /**
         * Sets the trace group fields
         * @param traceGroupFields
         * @since 1.2
         */
        public Builder withTraceGroupFields(final TraceGroupFields traceGroupFields) {
            checkNotNull(traceGroupFields, "traceGroupFields cannot be null");
            putAndMarkAsProvided(TRACE_GROUP_FIELDS_KEY, traceGroupFields);
            return this;
        }

        private Builder putAndMarkAsProvided(final String key, final Object value) {
            data.put(key, value);
            remainingRequiredFields.remove(key);
            return this;
        }

        /**
         * Returns a newly created {@link JacksonSpan}
         * @return a JacksonSpan
         * @since 1.2
         */
        public JacksonSpan build() {
            checkArgument(remainingRequiredFields.isEmpty(),
                    String.format("The following values were not provided and  are required: %s", remainingRequiredFields));
            this.withData(data);
            this.withEventType(EventType.TRACE.toString());
            return new JacksonSpan(this);
        }

    }
}
