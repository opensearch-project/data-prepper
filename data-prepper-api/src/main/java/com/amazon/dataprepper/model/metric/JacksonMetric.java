/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.metric;

import com.amazon.dataprepper.model.event.JacksonEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A Jackson implementation for {@link Metric}s.
 *
 * @since 1.4
 */
public abstract class JacksonMetric extends JacksonEvent implements Metric {

    protected static final String NAME_KEY = "name";
    protected static final String DESCRIPTION_KEY = "description";
    protected static final String START_TIME_KEY = "startTime";
    protected static final String TIME_KEY = "time";
    protected static final String SERVICE_NAME_KEY = "serviceName";
    protected static final String KIND_KEY = "kind";
    protected static final String UNIT_KEY = "unit";
    protected static final String ATTRIBUTES_KEY = "attributes";

    protected JacksonMetric(Builder builder) {
        super(builder);
    }

    @Override
    public String toJsonString() {
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

    @Override
    public String getServiceName() {
        return this.get(SERVICE_NAME_KEY, String.class);
    }

    @Override
    public String getName() {
        return this.get(NAME_KEY, String.class);
    }

    @Override
    public String getDescription() {
        return this.get(DESCRIPTION_KEY, String.class);
    }

    @Override
    public String getUnit() {
        return this.get(UNIT_KEY, String.class);
    }

    @Override
    public String getKind() {
        return this.get(KIND_KEY, String.class);
    }

    @Override
    public String getStartTime() {
        return this.get(START_TIME_KEY, String.class);
    }

    @Override
    public String getTime() {
        return this.get(TIME_KEY, String.class);
    }

    @Override
    public Map<String, Object> getAttributes() {
        return this.get(ATTRIBUTES_KEY, Map.class);
    }

    /**
     * Builder for creating {@link JacksonMetric}
     *
     * @since 1.4
     */
    public abstract static class Builder<T extends JacksonEvent.Builder<T>> extends JacksonEvent.Builder<T> {

        protected final Map<String, Object> data;

        public Builder() {
            data = new HashMap<>();
        }

        /**
         * Sets the kind of the event. One of {@link Metric.KIND}
         * @param kind the kind of this event
         * @return the builder
         * @since 1.4
         */
        public T withEventKind(final String kind) {
            data.put(KIND_KEY, kind);
            return getThis();
        }

        /**
         * Sets the unit of the event
         * @param unit the unit of this event
         * @return the builder
         * @since 1.4
         */
        public T withUnit(final String unit) {
            data.put(UNIT_KEY, unit);
            return getThis();
        }

        /**
         * Optional - sets the attributes for this event. Default is an empty map.
         * @param attributes the attributes to associate with this event.
         * @return the builder
         * @since 1.4
         */
        public T withAttributes(final Map<String, Object> attributes) {
            data.put(ATTRIBUTES_KEY, attributes);
            return getThis();
        }

        /**
         * Sets the gauge name
         * @param name the name
         * @return the builder
         * @since 1.4
         */
        public T withName(final String name) {
            data.put(NAME_KEY, name);
            return getThis();
        }

        /**
         * Sets the gauge description
         * @param description the description of the metric
         * @return the builder
         * @since 1.4
         */
        public T withDescription(final String description) {
            data.put(DESCRIPTION_KEY, description);
            return getThis();
        }

        /**
         * Sets the start time of the gauge
         * @param startTime
         * @return the builder
         * @since 1.4
         */
        public T withStartTime(final String startTime) {
            data.put(START_TIME_KEY, startTime);
            return getThis();
        }

        /**
         * Sets the time for the metricc event.
         * @param time the moment corresponding to when the data point's aggregate value was captured.
         * @return the builder
         * @since 1.4
         */
        public T withTime(final String time) {
            data.put(TIME_KEY, time);
            return getThis();
        }

        /**
         * Sets the service name of the metric event
         * @param serviceName sets the name of the service
         * @return the builder
         * @since 1.4
         */
        public T withServiceName(final String serviceName) {
            data.put(SERVICE_NAME_KEY, serviceName);
            return getThis();
        }
    }
}
