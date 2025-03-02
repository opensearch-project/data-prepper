/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A Jackson implementation for {@link Metric}s.
 *
 * @since 1.4
 */
public abstract class JacksonMetric extends JacksonEvent implements Metric {

    protected static final String NAME_KEY = "name";
    protected static final String DESCRIPTION_KEY = "description";
    protected static final String START_TIME_KEY = "startTime";
    protected static final String OTLP_START_TIME_KEY = "start_time";
    protected static final String TIME_KEY = "time";
    protected static final String SERVICE_NAME_KEY = "serviceName";
    protected static final String OTLP_SERVICE_NAME_KEY = "service_name";
    protected static final String KIND_KEY = "kind";
    protected static final String UNIT_KEY = "unit";
    public static final String ATTRIBUTES_KEY = "attributes";
    protected static final String SCHEMA_URL_KEY = "schemaUrl";
    protected static final String OTLP_SCHEMA_URL_KEY = "schema_url";
    protected static final String EXEMPLARS_KEY = "exemplars";
    protected static final String FLAGS_KEY = "flags";
    protected static final String SCOPE_KEY = "scope";
    protected static final String RESOURCE_KEY = "resource";
    private boolean opensearchMode;

    protected JacksonMetric(Builder builder, boolean opensearchMode) {
        super(builder);
        this.opensearchMode = opensearchMode;
    }

    public void setOpensearchMode(boolean opensearchMode) {
        this.opensearchMode = opensearchMode;
    }

    public boolean getOpensearchMode() {
        return opensearchMode;
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

    @Override
    public String getServiceName() {
        final String key = (opensearchMode) ? SERVICE_NAME_KEY : OTLP_SERVICE_NAME_KEY;
        return this.get(key, String.class);
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
        final String key = (opensearchMode) ? START_TIME_KEY : OTLP_START_TIME_KEY;
        return this.get(key, String.class);
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
    public Map<String, Object> getScope() {
        return this.get(SCOPE_KEY, Map.class);
    }

    @Override
    public Map<String, Object> getResource() {
        return this.get(RESOURCE_KEY, Map.class);
    }

    @Override
    public String getSchemaUrl() {
        final String key = (opensearchMode) ? SCHEMA_URL_KEY : OTLP_SCHEMA_URL_KEY;
        return this.get(key, String.class);
    }

    @Override
    public List<? extends Exemplar> getExemplars() {
        return this.getList(EXEMPLARS_KEY, DefaultExemplar.class);
    }

    @Override
    public Integer getFlags() {
        return this.get(FLAGS_KEY, Integer.class);
    }

    /**
     * Builder for creating {@link JacksonMetric}
     *
     * @since 1.4
     */
    public abstract static class Builder<T extends JacksonEvent.Builder<T>> extends JacksonEvent.Builder<T> {

        private final Map<String, Object> mdata;
        protected final boolean opensearchMode;

        public Builder(final boolean opensearchMode) {
            if (data == null) {
                data = new HashMap<String, Object>();
            }
            this.opensearchMode = opensearchMode;
            mdata = (HashMap<String, Object>)data;
            eventHandle = null;
        }

        public boolean getOpensearchMode() {
            return opensearchMode;
        }

        public void put(String key, Object value) {
            mdata.put(key, value);
        }

        public void computeIfAbsent(String key, Function<? super String,? extends Object> f) {
            mdata.computeIfAbsent(key, f);
        }

        /**
         * Sets the kind of the event. One of {@link Metric.KIND}
         * @param kind the kind of this event
         * @return the builder
         * @since 1.4
         */
        public T withEventKind(final String kind) {
            put(KIND_KEY, kind);
            return getThis();
        }

        /**
         * Sets the unit of the event
         * @param unit the unit of this event
         * @return the builder
         * @since 1.4
         */
        public T withUnit(final String unit) {
            put(UNIT_KEY, unit);
            return getThis();
        }

	public T withEventHandle(final EventHandle eventHandle) {
            this.eventHandle = eventHandle;
            return getThis();
	}

        /**
         * Optional - sets the attributes for this event. Default is an empty map.
         * @param attributes the attributes to associate with this event.
         * @return the builder
         * @since 1.4
         */
        public T withAttributes(final Map<String, Object> attributes) {
            put(ATTRIBUTES_KEY, attributes);
            return getThis();
        }

        public T withScope(final Map<String, Object> scope) {
            put(SCOPE_KEY, scope);
            return getThis();
        }

        public T withResource(final Map<String, Object> resource) {
            put(RESOURCE_KEY, resource);
            return getThis();
        }

        /**
         * Sets the gauge name
         * @param name the name
         * @return the builder
         * @since 1.4
         */
        public T withName(final String name) {
            put(NAME_KEY, name);
            return getThis();
        }

        /**
         * Sets the gauge description
         * @param description the description of the metric
         * @return the builder
         * @since 1.4
         */
        public T withDescription(final String description) {
            put(DESCRIPTION_KEY, description);
            return getThis();
        }

        /**
         * Sets the start time of the gauge
         * @param startTime the start time
         * @return the builder
         * @since 1.4
         */
        public T withStartTime(final String startTime) {
            final String key = (opensearchMode) ? START_TIME_KEY : OTLP_START_TIME_KEY;
            put(key, startTime);
            return getThis();
        }

        /**
         * Sets the time for the metricc event.
         * @param time the moment corresponding to when the data point's aggregate value was captured.
         * @return the builder
         * @since 1.4
         */
        public T withTime(final String time) {
            put(TIME_KEY, time);
            return getThis();
        }

        /**
         * Sets the service name of the metric event
         * @param serviceName sets the name of the service
         * @return the builder
         * @since 1.4
         */
        public T withServiceName(final String serviceName) {
            final String key = (opensearchMode) ? SERVICE_NAME_KEY : OTLP_SERVICE_NAME_KEY;
            put(key, serviceName);
            return getThis();
        }

        /**
         * Sets the schema url of the metric event
         * @param schemaUrl sets the url of the schema
         * @return the builder
         * @since 1.4
         */
        public T withSchemaUrl(final String schemaUrl) {
            final String key = (opensearchMode) ? SCHEMA_URL_KEY : OTLP_SCHEMA_URL_KEY;
            put(key, schemaUrl);
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
        public T withTimeReceived(final Instant timeReceived) {
            return (T)super.withTimeReceived(timeReceived);
        }


        /**
         * Sets the exemplars that are associated with this metric event
         * @param  exemplars sets the exemplars for this metric
         * @return the builder
         * @since 1.4
         */
        public T withExemplars(final List<Exemplar> exemplars) {
            put(EXEMPLARS_KEY, exemplars);
            return getThis();
        }

        /**
         * Sets the flags that are associated with this metric event
         * @param flags sets the flags for this metric
         * @return the builder
         * @since 1.4
         */
        public T withFlags(final Integer flags) {
            put(FLAGS_KEY, flags);
            return getThis();
        }

    }
}
