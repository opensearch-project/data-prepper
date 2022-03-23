/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.metric;

import com.amazon.dataprepper.model.event.EventType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A Jackson implementation for {@link Summary}.
 *
 * @since 1.4
 */
public class JacksonSummary extends JacksonMetric implements Summary {

    private static final String QUANTILES_KEY = "quantiles";
    private static final String QUANTILE_VALUES_COUNT_KEY = "quantileValuesCount";
    private static final String SUM_KEY = "sum";
    private static final String COUNT_KEY = "count";

    private static final List<String> REQUIRED_KEYS = Collections.singletonList(ATTRIBUTES_KEY);
    private static final List<String> REQUIRED_NON_EMPTY_KEYS = Arrays.asList(NAME_KEY, KIND_KEY, TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Collections.singletonList(COUNT_KEY);


    protected JacksonSummary(JacksonSummary.Builder builder) {
        super(builder);

        checkArgument(this.getMetadata().getEventType().equals(EventType.METRIC.toString()), "eventType must be of type Metric");
    }

    public static JacksonSummary.Builder builder() {
        return new JacksonSummary.Builder();
    }

    @Override
    public List<Quantile> getQuantiles() {
        return this.get(QUANTILES_KEY, List.class);
    }

    @Override
    public Integer getQuantileValuesCount() {
        return this.get(QUANTILE_VALUES_COUNT_KEY, Integer.class);
    }

    @Override
    public Double getSum() {
        return this.get(SUM_KEY, Double.class);
    }

    /**
     * Builder for creating JacksonSummary
     *
     * @since 1.4
     */
    public static class Builder extends JacksonMetric.Builder<JacksonSummary.Builder> {

        @Override
        public Builder getThis() {
            return this;
        }

        /**
         * Sets quantiles
         * @param quantiles a list of quantiles that is part of this metric record.
         * @return the builder
         * @since 1.4
         */
        public Builder withQuantiles(final List<Quantile> quantiles) {
            data.put(QUANTILES_KEY, quantiles);
            return this;
        }

        /**
         * Sets the quantiles count
         * @param quantileValuesCount the count of the quantiles in this record
         * @return the builder
         * @since 1.4
         */
        public Builder withQuantilesValueCount(int quantileValuesCount) {
            data.put(QUANTILE_VALUES_COUNT_KEY, quantileValuesCount);
            return this;
        }

        /**
         * Sets the sum
         * @param sum the sum of this summary
         * @return the builder
         * @since 1.4
         */
        public Builder withSum(double sum) {
            data.put(SUM_KEY, sum);
            return this;
        }

        /**
         * Sets the count
         * @param count the count of this summary
         * @return the builder
         * @since 1.4
         */
        public Builder withCount(Long count) {
            data.put(COUNT_KEY, count);
            return this;
        }

        /**
         * Returns a newly created {@link JacksonSummary}
         * @return a JacksonSummary
         * @since 1.4
         */
        public JacksonSummary build() {
            this.withData(data);
            this.withEventKind(KIND.SUMMARY.toString());
            this.withEventType(EventType.METRIC.toString());

            new ParameterValidator().validate(REQUIRED_KEYS, REQUIRED_NON_EMPTY_KEYS, REQUIRED_NON_NULL_KEYS, data);
            checkAndSetDefaultValues();
            return new JacksonSummary(this);
        }

        private void checkAndSetDefaultValues() {
            data.computeIfAbsent(ATTRIBUTES_KEY, k -> new HashMap<>());
        }

    }
}
