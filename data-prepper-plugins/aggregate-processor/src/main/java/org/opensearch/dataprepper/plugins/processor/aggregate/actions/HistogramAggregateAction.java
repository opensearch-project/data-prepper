/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.Bucket;
import org.opensearch.dataprepper.model.metric.Exemplar;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.metric.DefaultExemplar;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateProcessor;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;
import static org.opensearch.dataprepper.plugins.processor.aggregate.AggregateProcessor.getTimeNanos;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import static org.opensearch.dataprepper.plugins.processor.otelmetrics.OTelMetricsProtoHelper.createBuckets;
import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCommonUtils.convertUnixNanosToISO8601;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action will create a combined event with histogram buckets of the values
 * of specified list of keys from the groupState on concludeGroup.
 * @since 2.1
 */
@DataPrepperPlugin(name = "histogram", pluginType = AggregateAction.class, pluginConfigurationType = HistogramAggregateActionConfig.class)
public class HistogramAggregateAction implements AggregateAction {
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    private static final String EVENT_TYPE = "event";
    private final String countKey;
    private final String bucketCountsKey;
    private final String bucketsKey;
    private final String startTimeKey;
    private final String endTimeKey;
    private final OutputFormat outputFormat;
    private final String sumKey;
    private final String maxKey;
    private final String minKey;
    private final String durationKey;
    private final String key;
    private final String units;
    private final boolean recordMinMax;
    private Event minEvent;
    private Event maxEvent;
    private double minValue;
    private double maxValue;
    private final String metricName;

    private double[] buckets;

    @DataPrepperPluginConstructor
    public HistogramAggregateAction(final HistogramAggregateActionConfig histogramAggregateActionConfig) {
        this.key = histogramAggregateActionConfig.getKey();
        List<Number> bucketList = histogramAggregateActionConfig.getBuckets();
        this.buckets = new double[bucketList.size()+2];
        int bucketIdx = 0;
        this.metricName = histogramAggregateActionConfig.getMetricName();
        this.buckets[bucketIdx++] = -Float.MAX_VALUE;
        for (int i = 0; i < bucketList.size(); i++) {
            this.buckets[bucketIdx++] = convertToDouble(bucketList.get(i));
        }
        this.buckets[bucketIdx] = Float.MAX_VALUE;
        Arrays.sort(this.buckets);
        this.sumKey = histogramAggregateActionConfig.getSumKey();
        this.minKey = histogramAggregateActionConfig.getMinKey();
        this.maxKey = histogramAggregateActionConfig.getMaxKey();
        this.countKey = histogramAggregateActionConfig.getCountKey();
        this.bucketsKey = histogramAggregateActionConfig.getBucketsKey();
        this.bucketCountsKey = histogramAggregateActionConfig.getBucketCountsKey();
        this.startTimeKey = histogramAggregateActionConfig.getStartTimeKey();
        this.endTimeKey = histogramAggregateActionConfig.getEndTimeKey();
        this.durationKey = histogramAggregateActionConfig.getDurationKey();
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

    public Exemplar createExemplar(final String id, final Event event, double value) {
        long curTimeNanos = getTimeNanos(Instant.now());
        Map<String, Object> attributes = event.toMap();
        if (Objects.nonNull(id)) {
            attributes.put("exemplarId", id);
        }
        String spanId = null;
        String traceId = null;
        if (event instanceof Span) {
            Span span = (Span)event;
            spanId = span.getSpanId();
            traceId = span.getTraceId();
        }
        return new DefaultExemplar(convertUnixNanosToISO8601(curTimeNanos),
                    value,
                    spanId,
                    traceId,
                    attributes);
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
        Instant eventTime = Instant.now();
        Instant eventStartTime = eventTime;
        Instant eventEndTime = eventTime;
        final Object startTime = event.get(startTimeKey, Object.class);
        final Object endTime = event.get(endTimeKey, Object.class);
        if (startTime != null) {
            eventStartTime = AggregateProcessor.convertObjectToInstant(startTime);
        }
        if (endTime != null) {
            eventEndTime = AggregateProcessor.convertObjectToInstant(endTime);
        }
        if (groupState.get(bucketCountsKey) == null) {
            groupState.put(startTimeKey, eventStartTime);
            groupState.put(endTimeKey, eventEndTime);
            Long[] bucketCountsList = new Long[buckets.length-1];
            Arrays.fill(bucketCountsList, (long)0);
            bucketCountsList[idx]++;
            groupState.putAll(aggregateActionInput.getIdentificationKeys());
            groupState.put(sumKey, doubleValue);
            groupState.put(countKey, 1);
            groupState.put(bucketCountsKey, bucketCountsList);
            if (this.recordMinMax) {
                groupState.put(minKey, doubleValue);
                groupState.put(maxKey, doubleValue);
                minEvent = event;
                maxEvent = event;
                minValue = doubleValue;
                maxValue = doubleValue;
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
                    minEvent = event;
                    minValue = doubleValue;
                }
                double max = (double)groupState.get(maxKey);
                if (doubleValue > max) {
                    groupState.put(maxKey, doubleValue);
                    maxEvent = event;
                    maxValue = doubleValue;
                }
            }
            final Instant groupStartTime = (Instant)groupState.get(startTimeKey);
            final Instant groupEndTime = (Instant)groupState.get(endTimeKey);
            if (eventStartTime.isBefore(groupStartTime)) {
                groupState.put(startTimeKey, eventStartTime);
            }
            if (eventEndTime.isAfter(groupEndTime)) {
                groupState.put(endTimeKey, eventEndTime);
            }
        }
        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public AggregateActionOutput concludeGroup(final AggregateActionInput aggregateActionInput) {
        GroupState groupState = aggregateActionInput.getGroupState();
        Event event;
        Instant startTime = (Instant)groupState.get(startTimeKey);
        Instant endTime = (Instant)groupState.get(endTimeKey);
        long startTimeNanos = getTimeNanos(startTime);
        long endTimeNanos = getTimeNanos(endTime);
        String histogramKey = this.metricName + "_key";
        List<Exemplar> exemplarList = new ArrayList<>();
        exemplarList.add(createExemplar("min", minEvent, minValue));
        exemplarList.add(createExemplar("max", maxEvent, maxValue));
        if (outputFormat == OutputFormat.RAW) {
            groupState.put(histogramKey, key);
            groupState.put(durationKey, endTimeNanos-startTimeNanos);
            groupState.put(bucketsKey, Arrays.copyOfRange(this.buckets, 1, this.buckets.length-1));
            groupState.put(startTimeKey, startTime.atZone(ZoneId.of(ZoneId.systemDefault().toString())).format(DateTimeFormatter.ofPattern(DATE_FORMAT)));
            event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(groupState)
                .withEventHandle(aggregateActionInput.getEventHandle())
                .build();
        } else {
            List<Double> explicitBoundsList = new ArrayList<Double>();
            List<Long> bucketCounts = Arrays.asList((Long[])groupState.get(bucketCountsKey));
            for (int i = 1; i < this.buckets.length - 1; i++) {
                explicitBoundsList.add(this.buckets[i]);
            }
            List<Bucket> buckets = createBuckets(bucketCounts, explicitBoundsList);
            Map<String, Object> attr = new HashMap<>();
            aggregateActionInput.getIdentificationKeys().forEach((k, v) -> {
                attr.put((String)k, v);
            });
            attr.put(histogramKey, key);
            attr.put(durationKey, endTimeNanos-startTimeNanos);
            double sum = (double)groupState.get(sumKey);
            Double max = (Double)groupState.get(maxKey);
            Double min = (Double)groupState.get(minKey);
            Integer count = (Integer)groupState.get(countKey);
            String description = String.format("Histogram of %s in the events", key);
            JacksonHistogram histogram = JacksonHistogram.builder()
                .withName(this.metricName)
                .withDescription(description)
                .withTime(convertUnixNanosToISO8601(endTimeNanos))
                .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
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
                .withExemplars(exemplarList)
                .withAttributes(attr)
                .withEventHandle(aggregateActionInput.getEventHandle())
                .build(false);
            event = (Event)histogram;
        }

        return new AggregateActionOutput(List.of(event));
    }
}
