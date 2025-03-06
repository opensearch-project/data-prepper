/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Jackson implementation for {@link Metric}s.
 *
 * @since 2.11
 */
public abstract class JacksonStandardMetric extends JacksonEvent implements Metric {

    protected static final String NAME_KEY = "name";
    protected static final String SCOPE_KEY = "scope";
    protected static final String RESOURCE_KEY = "resource";
    protected static final String DESCRIPTION_KEY = "description";
    protected static final String START_TIME_KEY = "start_time";
    protected static final String TIME_KEY = "time";
    protected static final String SERVICE_NAME_KEY = "service_name";
    protected static final String KIND_KEY = "kind";
    protected static final String UNIT_KEY = "unit";
    protected static final String SUM_KEY = "sum";
    protected static final String VALUE_KEY = "value";
    protected static final String AGGREGATION_TEMPORALITY_KEY = "aggregation_temporality";
    protected static final String COUNT_KEY = "count";
    public static final String ATTRIBUTES_KEY = "attributes";
    protected static final String SCHEMA_URL_KEY = "schema_url";
    protected static final String EXEMPLARS_KEY = "exemplars";
    protected static final String FLAGS_KEY = "flags";

    protected JacksonStandardMetric(Builder builder) {
        super(builder);
    }

    @Override
    public String toJsonString() {
        return getJsonNode().toString();
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
    public Map<String, Object> getScope() {
        return this.get(SCOPE_KEY, Map.class);
    }

    @Override
    public Map<String, Object> getResource() {
        return this.get(RESOURCE_KEY, Map.class);
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
    public List<? extends Exemplar> getExemplars() {
        return this.getList(EXEMPLARS_KEY, DefaultExemplar.class);
    }

    @Override
    public Integer getFlags() {
        return this.get(FLAGS_KEY, Integer.class);
    }

    /**
     * Builder for creating {@link JacksonStandardMetric}
     *
     * @since 2.11
     */
    public abstract static class Builder<T extends JacksonEvent.Builder<T>> extends JacksonEvent.Builder<T> {

        private final Map<String, Object> mdata;

        public Builder() {
            if (data == null) {
                data = new HashMap<String, Object>();
            }
            mdata = (HashMap<String, Object>)data;
            eventHandle = null;
        }

        public void put(String key, Object value) {
                mdata.put(key, value);
        }

        /**
         * Sets the kind of the event. One of {@link Metric.KIND}
         * @param kind the kind of this event
         * @return the builder
         * @since 2.11
         */
        public T withEventKind(final String kind) {
            put(KIND_KEY, kind);
            return getThis();
        }

        /**
         * Sets the unit of the event
         * @param unit the unit of this event
         * @return the builder
         * @since 2.11
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
         * @since 2.11
         */
        public T withAttributes(final Map<String, Object> attributes) {
            put(ATTRIBUTES_KEY, attributes);
            return getThis();
        }

        /**
         * Sets the gauge name
         * @param name the name
         * @return the builder
         * @since 2.11
         */
        public T withName(final String name) {
            put(NAME_KEY, name);
            return getThis();
        }

        /**
         * Sets the gauge description
         * @param description the description of the metric
         * @return the builder
         * @since 2.11
         */
        public T withDescription(final String description) {
            put(DESCRIPTION_KEY, description);
            return getThis();
        }

        /**
         * Sets the scope of the log event
         *
         * @param scope scope to be set
         * @return the builder
         * @since 2.11
         */
        public T withScope(final Map<String, Object> scope) {
            put(SCOPE_KEY, scope);
            return getThis();
        }

        /**
         * Sets the resource of the log event
         *
         * @param resource resource to be set
         * @return the builder
         * @since 2.11
         */
        public T withResource(final Map<String, Object> resource) {
            put(RESOURCE_KEY, resource);
            return getThis();
        }

        /**
         * Sets the start time of the gauge
         * @param startTime the start time
         * @return the builder
         * @since 2.11
         */
        public T withStartTime(final String startTime) {
            put(START_TIME_KEY, startTime);
            return getThis();
        }

        /**
         * Sets the time for the metricc event.
         * @param time the moment corresponding to when the data point's aggregate value was captured.
         * @return the builder
         * @since 2.11
         */
        public T withTime(final String time) {
            put(TIME_KEY, time);
            return getThis();
        }

        /**
         * Sets the service name of the metric event
         * @param serviceName sets the name of the service
         * @return the builder
         * @since 2.11
         */
        public T withServiceName(final String serviceName) {
            put(SERVICE_NAME_KEY, serviceName);
            return getThis();
        }

        /**
         * Sets the schema url of the metric event
         * @param schemaUrl sets the url of the schema
         * @return the builder
         * @since 2.11
         */
        public T withSchemaUrl(final String schemaUrl) {
            put(SCHEMA_URL_KEY, schemaUrl);
            return getThis();
        }

        /**
         * Sets the time received for populating event origination time in event handle
         *
         * @param timeReceived time received
         * @return the builder
         * @since 2.11
         */
        @Override
        public T withTimeReceived(final Instant timeReceived) {
            return (T)super.withTimeReceived(timeReceived);
        }


        /**
         * Sets the exemplars that are associated with this metric event
         * @param  exemplars sets the exemplars for this metric
         * @return the builder
         * @since 2.11
         */
        public T withExemplars(final List<Exemplar> exemplars) {
            put(EXEMPLARS_KEY, exemplars);
            return getThis();
        }

        /**
         * Sets the flags that are associated with this metric event
         * @param flags sets the flags for this metric
         * @return the builder
         * @since 2.11
         */
        public T withFlags(final Integer flags) {
            put(FLAGS_KEY, flags);
            return getThis();
        }

    }
}
