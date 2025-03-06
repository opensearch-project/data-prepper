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
 * A Jackson implementation for {@link Histogram}.
 *
 * @since 2.11
 */
public class JacksonStandardExponentialHistogram extends JacksonStandardMetric implements ExponentialHistogram {

    private static final String SCALE_KEY = "scale";
    private static final String ZERO_COUNT_KEY = "zero_count";
    public static final String POSITIVE_BUCKETS_KEY = "positive_buckets";
    public static final String NEGATIVE_BUCKETS_KEY = "negative_buckets";
    private static final String NEGATIVE_KEY = "negative";
    private static final String POSITIVE_KEY = "positive";
    private static final String NEGATIVE_OFFSET_KEY = "negative_offset";
    private static final String POSITIVE_OFFSET_KEY = "positive_offset";

    private static final List<String> REQUIRED_KEYS = new ArrayList<>();
    private static final List<String> REQUIRED_NON_EMPTY_KEYS = Arrays.asList(NAME_KEY, KIND_KEY, TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Collections.singletonList(SUM_KEY);


    protected JacksonStandardExponentialHistogram(JacksonStandardExponentialHistogram.Builder builder) {
        super(builder);

        checkArgument(this.getMetadata().getEventType().equals(EventType.METRIC.toString()), "eventType must be of type Metric");
    }

    public static JacksonStandardExponentialHistogram.Builder builder() {
        return new JacksonStandardExponentialHistogram.Builder();
    }

    @Override
    public Double getSum() {
        return this.get(SUM_KEY, Double.class);
    }

    @Override
    public Long getCount() {
        return this.get(COUNT_KEY, Long.class);
    }

    @Override
    public String getAggregationTemporality() {
        return this.get(AGGREGATION_TEMPORALITY_KEY, String.class);
    }

    @Override
    public List<? extends Bucket> getNegativeBuckets() {
        return this.getList(NEGATIVE_BUCKETS_KEY, DefaultBucket.class);
    }

    @Override
    public List<? extends Bucket> getPositiveBuckets() {
        return this.getList(POSITIVE_BUCKETS_KEY, DefaultBucket.class);
    }

    @Override
    public List<Long> getNegative() {
        return this.getList(NEGATIVE_KEY, Long.class);
    }

    @Override
    public List<Long> getPositive() {
        return this.getList(POSITIVE_KEY, Long.class);
    }

    @Override
    public Long getZeroCount() {
        return this.get(ZERO_COUNT_KEY, Long.class);
    }

    @Override
    public Integer getScale() {
        return this.get(SCALE_KEY, Integer.class);
    }

    @Override
    public Integer getNegativeOffset() {
        return this.get(NEGATIVE_OFFSET_KEY, Integer.class);
    }

    @Override
    public Integer getPositiveOffset() {
        return this.get(POSITIVE_OFFSET_KEY, Integer.class);
    }

    /**
     * Builder for creating JacksonStandardExponentialHistogram
     *
     * @since 2.11
     */
    public static class Builder extends JacksonStandardMetric.Builder<JacksonStandardExponentialHistogram.Builder> {

        @Override
        public JacksonStandardExponentialHistogram.Builder getThis() {
            return this;
        }

        /**
         * Sets the sum of the histogram
         *
         * @param sum the sum of the histogram
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder withSum(double sum) {
            put(SUM_KEY, sum);
            return this;
        }

        /**
         * Sets the count of the histogram. Must be equal to the sum of the "count" fields in buckets
         *
         * @param count the number of values in the population
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder withCount(long count) {
            put(COUNT_KEY, count);
            return this;
        }

        /**
         * Sets the count of the histogram. Must be equal to the sum of the "count" fields in buckets
         *
         * @param scale the number of values in the population
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder withScale(int scale) {
            put(SCALE_KEY, scale);
            return this;
        }

        /**
         * Sets the count of values that are either exactly zero or within the region considered zero by the
         * instrumentation at the tolerated level of precision
         *
         * @param zeroCount the zeroCount
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder withZeroCount(long zeroCount) {
            put(ZERO_COUNT_KEY, zeroCount);
            return this;
        }

        /**
         * Sets the aggregation temporality for this histogram
         *
         * @param aggregationTemporality the aggregation temporality
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder withAggregationTemporality(String aggregationTemporality) {
            put(AGGREGATION_TEMPORALITY_KEY, aggregationTemporality);
            return this;
        }

        /**
         * Sets the positive range of calculated exponential buckets
         *
         * @param exponentialBuckets a list of buckets
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder  withPositiveBuckets(List<Bucket> exponentialBuckets) {
            put(POSITIVE_BUCKETS_KEY, exponentialBuckets);
            return this;
        }

        /**
         * Sets the negative range of exponential buckets
         *
         * @param exponentialBuckets a list of buckets
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder withNegativeBuckets(List<Bucket> exponentialBuckets) {
            put(NEGATIVE_BUCKETS_KEY, exponentialBuckets);
            return this;
        }

        /**
         * Sets the positive range of exponential bucket counts
         *
         * @param bucketCountsList positive bucket value counts
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder withPositive(List<Long> bucketCountsList) {
            put(POSITIVE_KEY, bucketCountsList);
            return this;
        }

        /**
         * Sets the negative range of exponential bucket counts
         *
         * @param bucketCountsList negative bucket value counts
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder withNegative(List<Long> bucketCountsList) {
            put(NEGATIVE_KEY, bucketCountsList);
            return this;
        }

        /**
         * Sets the offset for the positive buckets
         *
         * @param offset the offset
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder withPositiveOffset(int offset) {
            put(POSITIVE_OFFSET_KEY, offset);
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
        public JacksonStandardExponentialHistogram.Builder withTimeReceived(final Instant timeReceived) {
            return (JacksonStandardExponentialHistogram.Builder)super.withTimeReceived(timeReceived);
        }

        /**
         * Sets the offset for the negative buckets
         *
         * @param offset the offset
         * @return the builder
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram.Builder withNegativeOffset(int offset) {
            put(NEGATIVE_OFFSET_KEY, offset);
            return this;
        }

        /**
         * Returns a newly created {@link JacksonStandardExponentialHistogram}
         *
         * @return a JacksonStandardExponentialHistogram
         * @since 2.11
         */
        public JacksonStandardExponentialHistogram build() {
            this.withData(data);
            this.withEventKind(KIND.EXPONENTIAL_HISTOGRAM.toString());
            this.withEventType(EventType.METRIC.toString());

            return new JacksonStandardExponentialHistogram(this);
        }

    }
}
