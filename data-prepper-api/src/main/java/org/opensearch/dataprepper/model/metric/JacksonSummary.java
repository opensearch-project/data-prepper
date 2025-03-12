/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.metric;

import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.validation.ParameterValidator;

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
    private static final String SUM_KEY = "sum";
    private static final String COUNT_KEY = "count";

    private static final List<String> REQUIRED_KEYS = Collections.singletonList(ATTRIBUTES_KEY);
    private static final List<String> REQUIRED_NON_EMPTY_KEYS = Arrays.asList(NAME_KEY, KIND_KEY, TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Collections.emptyList();


    protected JacksonSummary(JacksonSummary.Builder builder, boolean flattenAttributes) {
        super(builder, flattenAttributes);
        checkAndSetDefaultValues();
        new ParameterValidator().validate(REQUIRED_KEYS, REQUIRED_NON_EMPTY_KEYS, REQUIRED_NON_NULL_KEYS, (HashMap<String, Object>)toMap());

        checkArgument(this.getMetadata().getEventType().equals(EventType.METRIC.toString()), "eventType must be of type Metric");
    }

    public static JacksonSummary.Builder builder() {
        return new JacksonSummary.Builder();
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
            put(QUANTILE_VALUES_COUNT_KEY, quantileValuesCount);
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
            return build(true);
        }

        /**
         * Returns a newly created {@link JacksonSummary}
         * @param flattenAttributes flag indicating if the attributes should be flattened or not
         * @return a JacksonSummary
         * @since 2.1
         */
        public JacksonSummary build(boolean flattenAttributes) {
            populateEvent(KIND.SUMMARY.toString());

            return new JacksonSummary(this, flattenAttributes);
        }


    }
}
