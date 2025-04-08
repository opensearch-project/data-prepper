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
 * A Jackson implementation for {@link Histogram}.
 *
 * @since 1.4
 */
public class JacksonHistogram extends JacksonMetric implements Histogram {

    private static final String SUM_KEY = "sum";
    private static final String MAX_KEY = "max";
    private static final String MIN_KEY = "min";
    private static final String COUNT_KEY = "count";
    private static final String AGGREGATION_TEMPORALITY_KEY = "aggregationTemporality";
    private static final String BUCKET_COUNTS_KEY = "bucketCounts";
    private static final String EXPLICIT_BOUNDS_COUNT_KEY = "explicitBoundsCount";
    public static final String BUCKETS_KEY = "buckets";
    private static final String BUCKET_COUNTS_LIST_KEY = "bucketCountsList";
    private static final String EXPLICIT_BOUNDS_KEY = "explicitBounds";

    private static final List<String> REQUIRED_KEYS = new ArrayList<>();
    private static final List<String> REQUIRED_NON_EMPTY_KEYS = Arrays.asList(NAME_KEY, KIND_KEY, TIME_KEY);
    private static final List<String> REQUIRED_NON_NULL_KEYS = Collections.singletonList(SUM_KEY);


    protected JacksonHistogram(JacksonHistogram.Builder builder, boolean flattenAttributes) {
        super(builder, flattenAttributes);
        checkAndSetDefaultValues();
        new ParameterValidator().validate(REQUIRED_KEYS, REQUIRED_NON_EMPTY_KEYS, REQUIRED_NON_NULL_KEYS, (HashMap<String, Object>)toMap());
        checkArgument(this.getMetadata().getEventType().equals(EventType.METRIC.toString()), "eventType must be of type Metric");
    }

    public static JacksonHistogram.Builder builder() {
        return new JacksonHistogram.Builder();
    }

    @Override
    public Double getSum() {
        return this.get(SUM_KEY, Double.class);
    }

    @Override
    public Double getMin() {
        return this.get(MIN_KEY, Double.class);
    }

    @Override
    public Double getMax() {
        return this.get(MAX_KEY, Double.class);
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
    public Integer getBucketCount() {
        return this.get(BUCKET_COUNTS_KEY, Integer.class);
    }

    @Override
    public Integer getExplicitBoundsCount() {
        return this.get(EXPLICIT_BOUNDS_COUNT_KEY, Integer.class);
    }

    @Override
    public List<? extends Bucket> getBuckets() {
        return this.getList(BUCKETS_KEY, DefaultBucket.class);
    }

    @Override
    public List<Long> getBucketCountsList() {
        return this.getList(BUCKET_COUNTS_LIST_KEY, Long.class);
    }

    @Override
    public List<Double> getExplicitBoundsList() {
        return this.getList(EXPLICIT_BOUNDS_KEY, Double.class);
    }

    /**
     * Builder for creating JacksonHistogram
     *
     * @since 1.4
     */
    public static class Builder extends JacksonMetric.Builder<JacksonHistogram.Builder> {

        @Override
        public JacksonHistogram.Builder getThis() {
            return this;
        }

        /**
         * Sets the sum of the histogram
         * @param sum the sum of the histogram
         * @return the builder
         * @since 1.4
         */
        public JacksonHistogram.Builder withSum(double sum) {
            put(SUM_KEY, sum);
            return this;
        }

        /**
         * Sets the min of the histogram
         * @param min the min of the histogram
         * @return the builder
         * @since 2.1
         */
        public JacksonHistogram.Builder withMin(Double min) {
            if (min != null) {
                put(MIN_KEY, min);
            }
            return this;
        }

        /**
         * Sets the max of the histogram
         * @param max the max of the histogram
         * @return the builder
         * @since 2.1
         */
        public JacksonHistogram.Builder withMax(Double max) {
            if (max != null) {
                put(MAX_KEY, max);
            }
            return this;
        }

        /**
         * Sets the count of the histogram. Must be equal to the sum of the "count" fields in buckets
         * @param count the number of values in the population
         * @return the builder
         * @since 1.4
         */
        public JacksonHistogram.Builder withCount(long count) {
            put(COUNT_KEY, count);
            return this;
        }

        /**
         * Sets the number of buckets for this histogram
         * @param bucketCount the number of buckets
         * @return the builder
         * @since 1.4
         */
        public JacksonHistogram.Builder withBucketCount(int bucketCount) {
            put(BUCKET_COUNTS_KEY, bucketCount);
            return this;
        }

        /**
         * Sets the bounds count for this histogram
         * @param explicitBoundsCount the count for the bounds
         * @return the builder
         * @since 1.4
         */
        public JacksonHistogram.Builder withExplicitBoundsCount(int explicitBoundsCount) {
            put(EXPLICIT_BOUNDS_COUNT_KEY, explicitBoundsCount);
            return this;
        }

        /**
         * Sets the aggregation temporality for this histogram
         * @param aggregationTemporality the aggregation temporality
         * @return the builder
         * @since 1.4
         */
        public  JacksonHistogram.Builder withAggregationTemporality(String aggregationTemporality) {
            put(AGGREGATION_TEMPORALITY_KEY, aggregationTemporality);
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
        public JacksonHistogram.Builder withTimeReceived(final Instant timeReceived) {
            return (JacksonHistogram.Builder)super.withTimeReceived(timeReceived);
        }

        /**
         * Sets the buckets for this histogram
         * @param buckets a list of buckets
         * @return the builder
         * @since 1.4
         */
        public JacksonHistogram.Builder withBuckets(List<Bucket> buckets) {
            put(BUCKETS_KEY, buckets);
            return this;
        }

        /**
         * Sets the buckets counts for this histogram
         * @param bucketCountsList the list with the individual counts
         * @return the builder
         * @since 1.4
         */
        public JacksonHistogram.Builder withBucketCountsList(List<Long> bucketCountsList) {
            put(BUCKET_COUNTS_LIST_KEY, bucketCountsList);
            return this;
        }

        /**
         * Sets the buckets bounds for this histogram
         * @param explicitBoundsList the list with the individual bucket bounds
         * @return the builder
         * @since 1.4
         */
        public JacksonHistogram.Builder withExplicitBoundsList(List<Double> explicitBoundsList) {
            put(EXPLICIT_BOUNDS_KEY, explicitBoundsList);
            return this;
        }

        /**
         * Returns a newly created {@link JacksonHistogram}
         * @return a JacksonHistogram
         * @since 1.4
         */
        public JacksonHistogram build() {
            return build(true);
        }

        /**
         * Returns a newly created {@link JacksonHistogram}
         * @param flattenAttributes flag indicating if the attributes should be flattened or not
         * @return a JacksonHistogram
         * @since 2.1
         */
        public JacksonHistogram build(boolean flattenAttributes) {
            populateEvent(KIND.HISTOGRAM.toString());

            return new JacksonHistogram(this, flattenAttributes);
        }


    }
}
