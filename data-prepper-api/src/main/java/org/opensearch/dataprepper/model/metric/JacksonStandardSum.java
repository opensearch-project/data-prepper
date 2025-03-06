/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import org.opensearch.dataprepper.model.event.EventType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jackson implementation for {@link Sum}.
 *
 * @since 2.11
 */
public class JacksonStandardSum extends JacksonStandardMetric implements Sum {

    private static final String IS_MONOTONIC_KEY = "is_monotonic";

    private static final List<String> REQUIRED_KEYS = new ArrayList<>();
    private static final List<String> REQUIRED_NON_EMPTY_KEYS = Arrays.asList(NAME_KEY, KIND_KEY, TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Arrays.asList(VALUE_KEY, IS_MONOTONIC_KEY);

    protected JacksonStandardSum(JacksonStandardSum.Builder builder) {
        super(builder);

        checkArgument(this.getMetadata().getEventType().equals(EventType.METRIC.toString()), "eventType must be of type Metric");
    }

    public static JacksonStandardSum.Builder builder() {
        return new JacksonStandardSum.Builder();
    }

    @Override
    public Double getValue() {
        return this.get(VALUE_KEY, Double.class);
    }

    @Override
    public String getAggregationTemporality() {
        return this.get(AGGREGATION_TEMPORALITY_KEY, String.class);
    }

    @Override
    public boolean isMonotonic() {
        return this.get(IS_MONOTONIC_KEY, Boolean.class);
    }

    /**
     * Builder for creating {@link JacksonStandardSum}
     *
     * @since 2.11
     */
    public static class Builder extends JacksonStandardMetric.Builder<JacksonStandardSum.Builder> {

        @Override
        public Builder getThis() {
            return this;
        }

        /**
         * Sets the value of the sum
         * @param value the measured value
         * @return the builder
         * @since 2.11
         */
        public Builder withValue(final Double value) {
            if (value != null) {
                put(VALUE_KEY, value);
            }
            return this;
        }

        /**
         * Sets the aggregation temporality
         * @param aggregationTemporality the aggregation temporality to associate with this event.
         * @return the builder
         * @since 2.11
         */
        public Builder withAggregationTemporality(String aggregationTemporality) {
            put(AGGREGATION_TEMPORALITY_KEY, aggregationTemporality);
            return this;
        }

        /**
         * Sets the monotonic property
         * @param isMonotonic true if this metric event is monotonic, false otherwise.
         * @return the builder
         * @since 2.11
         */
        public Builder withIsMonotonic(final boolean isMonotonic) {
            put(IS_MONOTONIC_KEY, isMonotonic);
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
         * Returns a newly created {@link JacksonStandardSum}
         * @return a JacksonStandardSum
         * @since 2.11
         */
        public JacksonStandardSum build() {
            this.withData(data);
            this.withEventType(EventType.METRIC.toString());
            this.withEventKind(Metric.KIND.SUM.toString());

            return new JacksonStandardSum(this);
        }

    }
}
