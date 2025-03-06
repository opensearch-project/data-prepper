/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import org.opensearch.dataprepper.model.event.EventType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jackson implementation for {@link Gauge}.
 *
 * @since 2.11
 */
public class JacksonStandardGauge extends JacksonStandardMetric implements Gauge {

    private static final List<String> REQUIRED_KEYS = new ArrayList<>();
    private static final List<String> REQUIRED_NON_EMPTY_KEYS = Arrays.asList(NAME_KEY, KIND_KEY, TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Collections.singletonList(VALUE_KEY);

    protected JacksonStandardGauge(Builder builder) {
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
     * Builder for creating {@link JacksonStandardGauge}
     *
     * @since 2.11
     */
    public static class Builder extends JacksonStandardMetric.Builder<JacksonStandardGauge.Builder> {

        @Override
        public Builder getThis() {
            return this;
        }

        /**
         * Sets the value of the gauge
         * @param value the value of the gauge
         * @return returns the builder
         * @since 2.11
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
         * @since 2.11
         */
        @Override
        public Builder withTimeReceived(final Instant timeReceived) {
            return (Builder)super.withTimeReceived(timeReceived);
        }

        /**
         * Returns a newly created {@link JacksonStandardGauge}
         * @return a JacksonStandardGauge
         * @since 2.11
         */
        public JacksonStandardGauge build() {
            this.withEventKind(Metric.KIND.GAUGE.toString());
            this.withData(data);
            this.withEventType(EventType.METRIC.toString());
            return new JacksonStandardGauge(this);
        }

    }
}
