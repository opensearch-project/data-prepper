/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.Bucket;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;
import static org.opensearch.dataprepper.plugins.processor.aggregate.AggregateProcessor.getTimeNanos;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import static org.opensearch.dataprepper.plugins.processor.otelmetrics.OTelMetricsProtoHelper.createBuckets;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Optional;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action will create a combined event with histogram buckets of the values 
 * of specified list of keys from the groupState on concludeGroup. 
 * @since 2.1
 */
@DataPrepperPlugin(name = "histogram", pluginType = AggregateAction.class, pluginConfigurationType = HistogramAggregateActionConfig.class)
public class HistogramAggregateAction implements AggregateAction {
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    static final String EVENT_TYPE = "event";
    public static final String HISTOGRAM_METRIC_NAME = "histogram";
    public static final String SUM_KEY = "sum";
    public static final String COUNT_KEY = "count";
    public static final String BUCKETS_KEY = "buckets";
    public static final String BUCKET_COUNTS_KEY = "bucket_counts";
    public static final String MIN_KEY = "min";
    public static final String MAX_KEY = "max";
    public static final String START_TIME_KEY = "startTime";
    public final String countKey;
    public final String bucketCountsKey;
    public final String bucketsKey;
    public final String startTimeKey;
    public final String outputFormat;
    public final String sumKey;
    public final String maxKey;
    public final String minKey;
    public final String key;
    public final String units;
    public final String keyPrefix;
    public final boolean recordMinMax;

    private long startTimeNanos;
    private double[] buckets;

    @DataPrepperPluginConstructor
    public HistogramAggregateAction(final HistogramAggregateActionConfig histogramAggregateActionConfig) {
        this.key = histogramAggregateActionConfig.getKey();
        List<Number> bucketList = histogramAggregateActionConfig.getBuckets();
        this.buckets = new double[bucketList.size()+2];
        int bucketIdx = 0;
        this.buckets[bucketIdx++] = -Float.MAX_VALUE;
        for (int i = 0; i < bucketList.size(); i++) {
            this.buckets[bucketIdx++] = convertToDouble(bucketList.get(i));
        }
        this.buckets[bucketIdx] = Float.MAX_VALUE;
        Arrays.sort(this.buckets);
        this.keyPrefix = histogramAggregateActionConfig.getGeneratedKeyPrefix();
        this.bucketCountsKey = this.keyPrefix + BUCKET_COUNTS_KEY;
        this.bucketsKey = this.keyPrefix + BUCKETS_KEY;
        this.countKey = this.keyPrefix + COUNT_KEY;
        this.sumKey = this.keyPrefix + SUM_KEY;
        this.maxKey = this.keyPrefix + MAX_KEY;
        this.minKey = this.keyPrefix + MIN_KEY;
        this.startTimeKey = this.keyPrefix + START_TIME_KEY;
        this.outputFormat = histogramAggregateActionConfig.getOutputFormat();
        this.units = histogramAggregateActionConfig.getUnits();
        this.recordMinMax = histogramAggregateActionConfig.getRecordMinMax();
    }

    private double convertToDouble(Number value) {
        double doubleValue;
        if (value instanceof Long) {
            doubleValue = (double)value.longValue();
        } else if (value instanceof Integer) {
            doubleValue = (double)value.intValue();
        } else if (value instanceof Short) {
            doubleValue = (double)value.shortValue();
        } else if (value instanceof Byte) {
            doubleValue = (double)value.byteValue();
        } else if (value instanceof Float) {
            doubleValue = (double)value.floatValue();
        } else {
            doubleValue = value.doubleValue();
        }
        return doubleValue;
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        Number value = event.get(key, Number.class);
        if (value == null) {
            return AggregateActionResponse.nullEventResponse();
        }
        double doubleValue = convertToDouble(value);
        
        int idx = Arrays.binarySearch(this.buckets, doubleValue);
        if (idx < 0) {
            idx = -idx-2;
        }
        if (groupState.get(bucketCountsKey) == null) {
            Long[] bucketCountsList = new Long[buckets.length-1];
            Arrays.fill(bucketCountsList, (long)0);
            bucketCountsList[idx]++;
            groupState.put(startTimeKey, Instant.now());
            groupState.putAll(aggregateActionInput.getIdentificationKeys());
            groupState.put(keyPrefix + "key", key);
            groupState.put(sumKey, doubleValue);
            groupState.put(countKey, 1);
            groupState.put(bucketCountsKey, bucketCountsList);
            if (this.recordMinMax) {
                groupState.put(minKey, doubleValue);
                groupState.put(maxKey, doubleValue);
            }
        } else {
            Integer v = (Integer)groupState.get(countKey) + 1;
            groupState.put(countKey, v);
            double sum = (double)groupState.get(sumKey);
            groupState.put(sumKey, sum+doubleValue);
            Long[] bucketCountsList = (Long[])groupState.get(bucketCountsKey);
            bucketCountsList[idx]++;
            if (this.recordMinMax) {
                double min = (double)groupState.get(minKey);
                if (doubleValue < min) {
                    groupState.put(minKey, doubleValue);
                }
                double max = (double)groupState.get(maxKey);
                if (doubleValue > max) {
                    groupState.put(maxKey, doubleValue);
                }
            }
        } 
        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public Optional<Event> concludeGroup(final AggregateActionInput aggregateActionInput) {
        GroupState groupState = aggregateActionInput.getGroupState();
        Event event;
        Instant startTime = (Instant)groupState.get(startTimeKey);
        if (outputFormat.equals(OutputFormat.RAW.toString())) {
            groupState.put(bucketsKey, Arrays.copyOfRange(this.buckets, 1, this.buckets.length-1));
            groupState.put(startTimeKey, startTime.atZone(ZoneId.of(ZoneId.systemDefault().toString())).format(DateTimeFormatter.ofPattern(DATE_FORMAT)));
            event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(groupState)
                .build();
        } else {
            List<Double> explicitBoundsList = new ArrayList<Double>();
            List<Long> bucketCounts = Arrays.asList((Long[])groupState.get(bucketCountsKey));
            for (int i = 1; i < this.buckets.length - 1; i++) {
                explicitBoundsList.add(this.buckets[i]);
            }
            List<Bucket> buckets = createBuckets(bucketCounts, explicitBoundsList);
            Integer countValue = (Integer)groupState.get(countKey);
            long currentTimeNanos = getTimeNanos(Instant.now());
            long startTimeNanos = getTimeNanos(startTime);
            Map<String, Object> attr = new HashMap<String, Object>();
            groupState.forEach((k, v) -> {
                if (((String)k).indexOf(keyPrefix) != 0) {
                    attr.put((String)k, v);
                }
            });
            attr.put("key", key);
            double sum = (double)groupState.get(sumKey);
            Double max = (Double)groupState.get(maxKey);
            Double min = (Double)groupState.get(minKey);
            Integer count = (Integer)groupState.get(countKey);
            String description = String.format("Histogram of %s in the events", key);
            JacksonHistogram histogram = JacksonHistogram.builder()
                .withName(HISTOGRAM_METRIC_NAME)
                .withDescription(description)
                .withTime(OTelProtoCodec.convertUnixNanosToISO8601(currentTimeNanos))
                .withStartTime(OTelProtoCodec.convertUnixNanosToISO8601(startTimeNanos))
                .withUnit(this.units)
                .withSum(sum)
                .withMin(min)
                .withMax(max)
                .withCount(count)
                .withBucketCount(this.buckets.length-1)
                .withExplicitBoundsCount(this.buckets.length-2)
                .withAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA.name())
                .withBuckets(buckets)
                .withBucketCountsList(bucketCounts)
                .withExplicitBoundsList(explicitBoundsList)
                .withAttributes(attr)
                .build();
            event = (Event)histogram;
        }
        
        return Optional.of(event);
    }
}
