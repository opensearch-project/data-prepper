/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.validation.ParameterValidator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jackson implementation for {@link Gauge}.
 *
 * @since 1.4
 */
public class JacksonGauge extends JacksonMetric implements Gauge {

    private static final String VALUE_KEY = "value";

    private static final List<String> REQUIRED_KEYS = new ArrayList<>();
    private static final List<String> REQUIRED_NON_EMPTY_KEYS = Arrays.asList(NAME_KEY, KIND_KEY, TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Collections.singletonList(VALUE_KEY);

    protected JacksonGauge(Builder builder, boolean flattenAttributes) {
        super(builder, flattenAttributes);
        checkAndSetDefaultValues();
        new ParameterValidator().validate(REQUIRED_KEYS, REQUIRED_NON_EMPTY_KEYS, REQUIRED_NON_NULL_KEYS, (HashMap<String, Object>)toMap());
        checkArgument(this.getMetadata().getEventType().equals(EventType.METRIC.toString()), "eventType must be of type Metric");
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Double getValue() {
        return this.get(VALUE_KEY, Double.class);
    }

    /**
     * Builder for creating {@link JacksonGauge}
     *
     * @since 1.4
     */
    public static class Builder extends JacksonMetric.Builder<JacksonGauge.Builder> {

        @Override
        public Builder getThis() {
            return this;
        }

        /**
         * Sets the value of the gauge
         * @param value the value of the gauge
         * @return returns the builder
         * @since 1.4
         */
        public Builder withValue(final Double value) {
            if (value != null) {
                put(VALUE_KEY, value);
            }
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
         * Returns a newly created {@link JacksonGauge}
         * @return a JacksonGauge
         * @since 1.4
         */
        public JacksonGauge build() {
            return build(true);
        }

        /**
         * Returns a newly created {@link JacksonGauge}
         * @param flattenAttributes flag indicating if the attributes should be flattened or not
         * @return a JacksonGauge
         * @since 2.1
         */
        public JacksonGauge build(boolean flattenAttributes) {
            populateEvent(KIND.GAUGE.toString());
            return new JacksonGauge(this, flattenAttributes);
        }

    }
}
