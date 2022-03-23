/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.metric;

import com.amazon.dataprepper.model.event.EventType;

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


    protected JacksonGauge(Builder builder) {
        super(builder);
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
         * @since 1.4
         */
        public Builder withValue(final Double value) {
            if (value != null) {
                data.put(VALUE_KEY, value);
            }
            return this;
        }

        /**
         * Returns a newly created {@link JacksonGauge}
         * @return a JacksonGauge
         * @since 1.4
         */
        public JacksonGauge build() {
            this.withEventKind(Metric.KIND.GAUGE.toString());
            this.withData(data);
            this.withEventType(EventType.METRIC.toString());
            checkAndSetDefaultValues();
            new ParameterValidator().validate(REQUIRED_KEYS, REQUIRED_NON_EMPTY_KEYS, REQUIRED_NON_NULL_KEYS, data);
            return new JacksonGauge(this);
        }

        private void checkAndSetDefaultValues() {
            data.computeIfAbsent(ATTRIBUTES_KEY, k -> new HashMap<>());
        }
    }
}