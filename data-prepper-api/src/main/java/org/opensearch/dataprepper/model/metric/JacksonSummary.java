/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import org.opensearch.dataprepper.model.event.EventType;

import java.time.Instant;
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
    private static final String OTLP_QUANTILE_VALUES_COUNT_KEY = "quantile_values_count";
    private static final String SUM_KEY = "sum";
    private static final String COUNT_KEY = "count";

    private static final List<String> REQUIRED_KEYS = Collections.singletonList(ATTRIBUTES_KEY);
    private static final List<String> REQUIRED_NON_EMPTY_KEYS = Arrays.asList(NAME_KEY, KIND_KEY, TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Collections.emptyList();


    protected JacksonSummary(JacksonSummary.Builder builder, boolean opensearchMode) {
        super(builder, opensearchMode);

        checkArgument(this.getMetadata().getEventType().equals(EventType.METRIC.toString()), "eventType must be of type Metric");
    }

    public static JacksonSummary.Builder builder(final boolean opensearchMode) {
        return new JacksonSummary.Builder(opensearchMode);
    }

    @Override
    public List<? extends Quantile> getQuantiles() {
        return this.getList(QUANTILES_KEY, DefaultQuantile.class);
    }

    @Override
    public Long getCount() {
        return this.get(COUNT_KEY, Long.class);
    }

    @Override
    public Integer getQuantileValuesCount() {
        final String key = getOpensearchMode() ? QUANTILE_VALUES_COUNT_KEY : OTLP_QUANTILE_VALUES_COUNT_KEY;
        return this.get(key, Integer.class);
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

        public Builder(final boolean opensearchMode) {
            super(opensearchMode);
        }

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
            put(QUANTILES_KEY, quantiles);
            return this;
        }

        /**
         * Sets the quantiles count
         * @param quantileValuesCount the count of the quantiles in this record
         * @return the builder
         * @since 1.4
         */
        public Builder withQuantilesValueCount(int quantileValuesCount) {
            final String key = getOpensearchMode() ? QUANTILE_VALUES_COUNT_KEY : OTLP_QUANTILE_VALUES_COUNT_KEY;
            put(key, quantileValuesCount);
            return this;
        }

        /**
         * Sets the sum
         * @param sum the sum of this summary
         * @return the builder
         * @since 1.4
         */
        public Builder withSum(double sum) {
            put(SUM_KEY, sum);
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
         * Sets the count
         * @param count the count of this summary
         * @return the builder
         * @since 1.4
         */
        public Builder withCount(Long count) {
            put(COUNT_KEY, count);
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

            if (getOpensearchMode()) {
                new ParameterValidator().validate(REQUIRED_KEYS, REQUIRED_NON_EMPTY_KEYS, REQUIRED_NON_NULL_KEYS, (HashMap<String, Object>)data);
            }
            checkAndSetDefaultValues();
            return new JacksonSummary(this, opensearchMode);
        }

        private void checkAndSetDefaultValues() {
            computeIfAbsent(ATTRIBUTES_KEY, k -> new HashMap<>());
        }

    }
}
