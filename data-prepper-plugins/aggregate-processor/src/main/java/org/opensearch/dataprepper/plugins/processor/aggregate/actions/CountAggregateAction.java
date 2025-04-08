/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.Exemplar;
import org.opensearch.dataprepper.model.metric.DefaultExemplar;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCommonUtils.convertUnixNanosToISO8601;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateProcessor;
import static org.opensearch.dataprepper.plugins.processor.aggregate.AggregateProcessor.getTimeNanos;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import org.opensearch.dataprepper.plugins.hasher.IdentificationKeysHasher;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action will count the number of events with same keys and will create a combined event
 * from the groupState on concludeGroup. The combined event will have identification keys, count and the start time either in one of the supported output formats.
 * @since 2.1
 */
@DataPrepperPlugin(name = "count", pluginType = AggregateAction.class, pluginConfigurationType = CountAggregateActionConfig.class)
public class CountAggregateAction implements AggregateAction {
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    private static final String UNIQUE_KEYS_SETKEY = "__unique_keys";
    private static final String exemplarKey = "__exemplar";
    static final String EVENT_TYPE = "event";
    static final String SUM_METRIC_DESCRIPTION = "Number of events";
    static final String SUM_METRIC_UNIT = "1";
    static final boolean SUM_METRIC_IS_MONOTONIC = true;
    public final String countKey;
    public final String startTimeKey;
    public final String endTimeKey;
    public final OutputFormat outputFormat;
    private long startTimeNanos;
    private final String metricName;
    private final IdentificationKeysHasher uniqueKeysHasher;

    @DataPrepperPluginConstructor
    public CountAggregateAction(final CountAggregateActionConfig countAggregateActionConfig) {
        this.countKey = countAggregateActionConfig.getCountKey();
        this.startTimeKey = countAggregateActionConfig.getStartTimeKey();
        this.endTimeKey = countAggregateActionConfig.getEndTimeKey();
        this.outputFormat = countAggregateActionConfig.getOutputFormat();
        this.metricName = countAggregateActionConfig.getMetricName();
        if (countAggregateActionConfig.getUniqueKeys() != null) {
            this.uniqueKeysHasher = new IdentificationKeysHasher(countAggregateActionConfig.getUniqueKeys());
        } else {
            this.uniqueKeysHasher = null;
        }
    }

    public Exemplar createExemplar(final Event event) {
        long curTimeNanos = getTimeNanos(Instant.now());
        Map<String, Object> attributes = event.toMap();
        String spanId = null;
        String traceId = null;
        if (event instanceof Span) {
            Span span = (Span)event;
            spanId = span.getSpanId();
            traceId = span.getTraceId();
        }
        return new DefaultExemplar(
                    convertUnixNanosToISO8601(curTimeNanos),
                    1.0,
                    spanId,
                    traceId,
                    attributes);
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        Instant eventStartTime = Instant.now();
        Instant eventEndTime = eventStartTime;
        Object startTime = event.get(startTimeKey, Object.class);
        Object endTime = event.get(endTimeKey, Object.class);

        if (startTime != null) {
            eventStartTime = AggregateProcessor.convertObjectToInstant(startTime);
        }
        if (endTime != null) {
            eventEndTime = AggregateProcessor.convertObjectToInstant(endTime);
        }
        if (groupState.get(countKey) == null) {
            groupState.putAll(aggregateActionInput.getIdentificationKeys());
            if (uniqueKeysHasher != null) {
                Set<IdentificationKeysHasher.IdentificationKeysMap> uniqueKeysMapSet = new HashSet<>();
            
                uniqueKeysMapSet.add(uniqueKeysHasher.createIdentificationKeysMapFromEvent(event));
                groupState.put(UNIQUE_KEYS_SETKEY, uniqueKeysMapSet);
            } 
            groupState.put(countKey, 1);
            groupState.put(exemplarKey, createExemplar(event));
            groupState.put(startTimeKey, eventStartTime);
            groupState.put(endTimeKey, eventEndTime);
        } else {
            Integer v = (Integer)groupState.get(countKey) + 1;
            
            if (uniqueKeysHasher != null) {
                Set<IdentificationKeysHasher.IdentificationKeysMap> uniqueKeysMapSet = (Set<IdentificationKeysHasher.IdentificationKeysMap>) groupState.get(UNIQUE_KEYS_SETKEY);
                uniqueKeysMapSet.add(uniqueKeysHasher.createIdentificationKeysMapFromEvent(event));
                v = uniqueKeysMapSet.size();
            }
            groupState.put(countKey, v);
            Instant groupStartTime = (Instant)groupState.get(startTimeKey);
            Instant groupEndTime = (Instant)groupState.get(endTimeKey);
            if (eventStartTime.isBefore(groupStartTime))
                groupState.put(startTimeKey, eventStartTime);
            if (eventEndTime.isAfter(groupEndTime))
                groupState.put(endTimeKey, eventEndTime);
        }
        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public AggregateActionOutput concludeGroup(final AggregateActionInput aggregateActionInput) {
        GroupState groupState = aggregateActionInput.getGroupState();
        Event event;
        Instant startTime = (Instant)groupState.get(startTimeKey);
        Instant endTime = (Instant)groupState.get(endTimeKey);
        groupState.remove(endTimeKey);
        groupState.remove(UNIQUE_KEYS_SETKEY);
        if (outputFormat == OutputFormat.RAW) {
            groupState.put(startTimeKey, startTime.atZone(ZoneId.of(ZoneId.systemDefault().toString())).format(DateTimeFormatter.ofPattern(DATE_FORMAT)));
            event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(groupState)
                .withEventHandle(aggregateActionInput.getEventHandle())
                .build();
        } else {
            Integer countValue = (Integer)groupState.get(countKey);
            Exemplar exemplar = (Exemplar)groupState.get(exemplarKey);
            groupState.remove(exemplarKey);
            groupState.remove(countKey);
            groupState.remove(startTimeKey);
            long endTimeNanos = getTimeNanos(endTime);
            long startTimeNanos = getTimeNanos(startTime);
            Map<String, Object> attr = new HashMap<String, Object>();
            groupState.forEach((k, v) -> attr.put((String)k, v));
            JacksonSum sum = JacksonSum.builder()
                .withName(this.metricName)
                .withDescription(SUM_METRIC_DESCRIPTION)
                .withTime(convertUnixNanosToISO8601(endTimeNanos))
                .withStartTime(convertUnixNanosToISO8601(startTimeNanos))
                .withIsMonotonic(SUM_METRIC_IS_MONOTONIC)
                .withUnit(SUM_METRIC_UNIT)
                .withAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA.name())
                .withValue((double)countValue)
                .withExemplars(List.of(exemplar))
                .withAttributes(attr)
                .withEventHandle(aggregateActionInput.getEventHandle())
                .build(false);
            event = (Event)sum;
        }

        return new AggregateActionOutput(List.of(event));
    }
}
