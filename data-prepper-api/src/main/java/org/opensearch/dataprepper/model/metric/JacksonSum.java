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
import java.util.HashMap;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jackson implementation for {@link Sum}.
 *
 * @since 1.4
 */
public class JacksonSum extends JacksonMetric implements Sum {

    private static final String VALUE_KEY = "value";
    private static final String AGGREGATION_TEMPORALITY_KEY = "aggregationTemporality";
    private static final String IS_MONOTONIC_KEY = "isMonotonic";

    private static final List<String> REQUIRED_KEYS = new ArrayList<>();
    private static final List<String> REQUIRED_NON_EMPTY_KEYS = Arrays.asList(NAME_KEY, KIND_KEY, TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Arrays.asList(VALUE_KEY, IS_MONOTONIC_KEY);

    protected JacksonSum(JacksonSum.Builder builder, boolean flattenAttributes) {
        super(builder, flattenAttributes);
        checkAndSetDefaultValues();
        new ParameterValidator().validate(REQUIRED_KEYS, REQUIRED_NON_EMPTY_KEYS, REQUIRED_NON_NULL_KEYS, (HashMap<String, Object>)toMap());

        checkArgument(this.getMetadata().getEventType().equals(EventType.METRIC.toString()), "eventType must be of type Metric");
    }

    public static JacksonSum.Builder builder() {
        return new JacksonSum.Builder();
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
     * Builder for creating {@link JacksonSum}
     *
     * @since 1.4
     */
    public static class Builder extends JacksonMetric.Builder<JacksonSum.Builder> {

        @Override
        public Builder getThis() {
            return this;
        }

        /**
         * Sets the value of the sum
         * @param value the measured value
         * @return the builder
         * @since 1.4
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
         * @since 1.4
         */
        public Builder withAggregationTemporality(String aggregationTemporality) {
            put(AGGREGATION_TEMPORALITY_KEY, aggregationTemporality);
            return this;
        }

        /**
         * Sets the monotonic property
         * @param isMonotonic true if this metric event is monotonic, false otherwise.
         * @return the builder
         * @since 1.4
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
         * @since 2.7
         */
        @Override
        public Builder withTimeReceived(final Instant timeReceived) {
            return (Builder)super.withTimeReceived(timeReceived);
        }

        /**
         * Returns a newly created {@link JacksonSum}
         * @return a JacksonSum
         * @since 1.4
         */
        public JacksonSum build() {
            return build(true);
        }

        /**
         * Returns a newly created {@link JacksonSum}
         * @param flattenAttributes flag indicating if the attributes should be flattened or not
         * @return a JacksonSum
         * @since 2.1
         */
        public JacksonSum build(boolean flattenAttributes) {
            populateEvent(KIND.SUM.toString());

            return new JacksonSum(this, flattenAttributes);
        }


    }
}
