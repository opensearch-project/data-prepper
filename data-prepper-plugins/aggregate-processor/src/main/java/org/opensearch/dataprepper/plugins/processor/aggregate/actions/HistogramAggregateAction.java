/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;
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
    static final String SUM_METRIC_NAME = "count";
    static final String SUM_METRIC_DESCRIPTION = "Number of events";
    static final String SUM_METRIC_UNIT = "1";
    static final boolean SUM_METRIC_IS_MONOTONIC = true;
    public final String countKey;
    public final String startTimeKey;
    public final String outputFormat;
    private long startTimeNanos;

    @DataPrepperPluginConstructor
    public HistogramAggregateAction(final HistogramAggregateActionConfig histogramAggregateActionConfig) {
        this.key = histogramAggregateActionConfig.getKey();
        this.type = histogramAggregateActionConfig.getType();
        this.buckets = histogramAggregateActionConfig.getBuckets();
        this.histogramKey = histogramAggregateActionConfig.getHistogramKey();
        this.startTimeKey = histogramAggregateActionConfig.getStartTimeKey();
        this.outputFormat = histogramAggregateActionConfig.getOutputFormat();
    }

    private long getTimeNanos(Instant time) {
        final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
        long currentTimeNanos = time.getEpochSecond() * NANO_MULTIPLIER + time.getNano();
        return currentTimeNanos;
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        NUMBER value = event.get(key, NUMBER.class);
        if (value == null) {
            return AggregateActionResponse.nullEventResponse();
        }
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
        
        if (groupState.get(histogramKey) == null) {
            groupState.put(startTimeKey, Instant.now());
            groupState.putAll(aggregateActionInput.getIdentificationKeys());
            groupState.put
            groupState.put(countKey, 1);
        } else {
            Integer v = (Integer)groupState.get(countKey) + 1;
            groupState.put(countKey, v);
        } 
        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public Optional<Event> concludeGroup(final AggregateActionInput aggregateActionInput) {
        GroupState groupState = aggregateActionInput.getGroupState();
        Event event;
        Instant startTime = (Instant)groupState.get(startTimeKey);
        if (outputFormat.equals(OutputFormat.RAW.toString())) {
            groupState.put(startTimeKey, startTime.atZone(ZoneId.of(ZoneId.systemDefault().toString())).format(DateTimeFormatter.ofPattern(DATE_FORMAT)));
            event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(groupState)
                .build();
        } else {
            Integer countValue = (Integer)groupState.get(countKey);
            groupState.remove(countKey);
            groupState.remove(startTimeKey);
            long currentTimeNanos = getTimeNanos(Instant.now());
            long startTimeNanos = getTimeNanos(startTime);
            Map<String, Object> attr = new HashMap<String, Object>();
            groupState.forEach((k, v) -> attr.put((String)k, v));
            JacksonSum js = JacksonSum.builder()
                .withName(SUM_METRIC_NAME)
                .withDescription(SUM_METRIC_DESCRIPTION)
                .withTime(OTelProtoCodec.convertUnixNanosToISO8601(currentTimeNanos))
                .withStartTime(OTelProtoCodec.convertUnixNanosToISO8601(startTimeNanos))
                .withIsMonotonic(SUM_METRIC_IS_MONOTONIC)
                .withUnit(SUM_METRIC_UNIT)
                .withAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE.name())
                .withValue((double)countValue)
                .withAttributes(attr)
                .build();
            event = (Event)js;
        }
        
        return Optional.of(event);
    }
}
