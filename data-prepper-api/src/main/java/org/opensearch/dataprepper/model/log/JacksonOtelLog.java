/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jackson implementation for {@link OpenTelemetryLog}.
 *
 * @since 2.1
 */
public class JacksonOtelLog extends JacksonEvent implements OpenTelemetryLog {

    protected static final String OBSERVED_TIME_KEY = "observedTimestamp";
    protected static final String SCOPE_KEY = "instrumentationScope";
    protected static final String RESOURCE_KEY = "resource";
    protected static final String TIME_KEY = "time";
    protected static final String SERVICE_NAME_KEY = "serviceName";
    protected static final String ATTRIBUTES_KEY = "attributes";
    protected static final String SCHEMA_URL_KEY = "schemaUrl";
    protected static final String FLAGS_KEY = "flags";
    protected static final String BODY_KEY = "body";
    protected static final String SPAN_ID_KEY = "spanId";
    protected static final String TRACE_ID_KEY = "traceId";
    protected static final String SEVERITY_KEY = "severity";
    protected static final String SEVERITY_NUMBER_KEY = "severityNumber";
    protected static final String SEVERITY_TEXT_KEY = "severityText";
    protected static final String DROPPED_ATTRIBUTES_COUNT_KEY = "droppedAttributesCount";

    protected void checkAndSetDefaultValues() {
        putIfAbsent(ATTRIBUTES_KEY, Map.class, new HashMap<>());
    }

    protected JacksonOtelLog(final JacksonOtelLog.Builder builder) {
        super(builder);
        checkAndSetDefaultValues();

        checkArgument(this.getMetadata().getEventType().equals("LOG"), "eventType must be of type Log");
    }

    @Override
    public String getServiceName() {
        return this.get(SERVICE_NAME_KEY, String.class);
    }

    @Override
    public String getObservedTime() {
        return this.get(OBSERVED_TIME_KEY, String.class);
    }

    @Override
    public String getTime() {
        return this.get(TIME_KEY, String.class);
    }

    @Override
    public Map<String, Object> getAttributes() {
        return this.get(ATTRIBUTES_KEY, Map.class);
    }

    @Override
    public String getSchemaUrl() {
        return this.get(SCHEMA_URL_KEY, String.class);
    }

    @Override
    public Integer getFlags() {
        return this.get(FLAGS_KEY, Integer.class);
    }

    @Override
    public String getSpanId() {
        return this.get(SPAN_ID_KEY, String.class);
    }

    @Override
    public String getTraceId() {
        return this.get(TRACE_ID_KEY, String.class);
    }

    @Override
    public Integer getSeverityNumber() {
        return this.get(SEVERITY_NUMBER_KEY, Integer.class);
    }

    @Override
    public String getSeverityText() {
        return this.get(SEVERITY_TEXT_KEY, String.class);
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
    public Integer getDroppedAttributesCount() {
        return this.get(DROPPED_ATTRIBUTES_COUNT_KEY, Integer.class);
    }

    @Override
    public Object getBody() {
        return this.get(BODY_KEY, Object.class);
    }

    /**
     * Constructs an empty builder.
     *
     * @return a builder
     * @since 2.1
     */
    public static JacksonOtelLog.Builder builder() {
        return new JacksonOtelLog.Builder();
    }

    @Override
    public String toJsonString() {
        Object anyAttributes = getJsonNode().get("attributes");
        if(anyAttributes instanceof ObjectNode) {
            final ObjectNode flattenedJsonNode = getJsonNode().deepCopy();
            flattenedJsonNode.remove("attributes");

            for (Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) anyAttributes).fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                String field = entry.getKey();
                if (!flattenedJsonNode.has(field)) {
                    flattenedJsonNode.set(field, entry.getValue());
                }
            }
            return flattenedJsonNode.toString();
        }
        return super.toJsonString();
    }
    /**
     * Builder for creating {@link JacksonLog}.
     *
     * @since 2.1
     */
    public static class Builder extends JacksonEvent.Builder<JacksonOtelLog.Builder> {

        protected final Map<String, Object> data;

        public Builder() {
            data = new HashMap<>();
        }

        @Override
        public JacksonOtelLog.Builder getThis() {
            return this;
        }

        /**
         * Optional - sets the attributes for this event. Default is an empty map.
         *
         * @param attributes the attributes to associate with this event.
         * @return the builder
         * @since 2.1
         */
        public Builder withAttributes(final Map<String, Object> attributes) {
            data.put(ATTRIBUTES_KEY, attributes);
            return getThis();
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
         * Sets the observed time of the log event
         *
         * @param observedTime the start time
         * @return the builder
         * @since 2.1
         */
        public Builder withObservedTime(final String observedTime) {
            data.put(OBSERVED_TIME_KEY, observedTime);
            return getThis();
        }

        /**
         * Sets the time for the log event.
         *
         * @param time the moment corresponding to when the data point's aggregate value was captured.
         * @return the builder
         * @since 2.1
         */
        public Builder withTime(final String time) {
            data.put(TIME_KEY, time);
            return getThis();
        }

        /**
         * Sets the service name of the log event
         * @param serviceName sets the name of the service
         * @return the builder
         * @since 2.1
         */
        public Builder withServiceName(final String serviceName) {
            data.put(SERVICE_NAME_KEY, serviceName);
            return getThis();
        }

        /**
         * Sets the schema url of the log event
         *
         * @param schemaUrl sets the url of the schema
         * @return the builder
         * @since 2.1
         */
        public Builder withSchemaUrl(final String schemaUrl) {
            data.put(SCHEMA_URL_KEY, schemaUrl);
            return getThis();
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
         * Sets the flags that are associated with this log event
         *
         * @param flags sets the flags for this log
         * @return the builder
         * @since 2.1
         */
        public Builder withFlags(final Integer flags) {
            data.put(FLAGS_KEY, flags);
            return getThis();
        }

        /**
         * Sets the body that are associated with this log event
         *
         * @param body sets the body of this log event
         * @return the builder
         * @since 2.1
         */
        public Builder withBody(final Object body) {
            data.put(BODY_KEY, body);
            return getThis();
        }

        /**
         * Sets the span id if a span is associated with this log event
         *
         * @param spanId sets the span id of this log event
         * @return the builder
         * @since 2.1
         */
        public Builder withSpanId(final String spanId) {
            data.put(SPAN_ID_KEY, spanId);
            return getThis();
        }

        /**
         * Sets the trace id if a trace is associated with this log event
         *
         * @param traceId sets trace id of this log event
         * @return the builder
         * @since 2.1
         */
        public Builder withTraceId(final String traceId) {
            data.put(TRACE_ID_KEY, traceId);
            return getThis();
        }

        /**
         * Sets the severity number of this log event, uses its numerical value
         *
         * @param severityNumber sets the severity number of this log event
         * @return the builder
         * @since 2.1
         */
        public Builder withSeverityNumber(final Integer severityNumber) {
            data.put(SEVERITY_NUMBER_KEY, severityNumber);
            return getThis();
        }

        /**
         * Sets the severity text of this log event
         *
         * @param severityText sets the severity text of this log event
         * @return the builder
         * @since 2.5
         */
        public Builder withSeverityText(final String severityText) {
            data.put(SEVERITY_TEXT_KEY, severityText);
            return getThis();
        }

        /**
         * Sets the dropped attributes count of this log event
         *
         * @param droppedAttributesCount sets the dropped attributes count of this log event
         * @return the builder
         * @since 2.1
         */
        public Builder withDroppedAttributesCount(final Integer droppedAttributesCount) {
            data.put(DROPPED_ATTRIBUTES_COUNT_KEY, droppedAttributesCount);
            return getThis();
        }

        protected void populateEvent() {
            this.withEventType(EventType.LOG.toString());
            this.withData(data);
        }

        /**
         * Returns a newly created {@link JacksonOtelLog}.
         *
         * @return a log
         * @since 2.1
         */
        public JacksonOtelLog build() {
            populateEvent();
            return new JacksonOtelLog(this);
        }
    }
}
