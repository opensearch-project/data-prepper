/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Sums the numeric value of a configured key for events in the same group and emits the total on concludeGroup.
 */
@DataPrepperPlugin(name = "sum", pluginType = AggregateAction.class, pluginConfigurationType = SumAggregateActionConfig.class)
public class SumAggregateAction implements AggregateAction {
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    private static final String EXEMPLAR_KEY = "__exemplar";
    private static final String SUM_KEY = "aggr._sum";
    private static final String START_TIME_KEY = "aggr._start_time";
    private static final String END_TIME_KEY = "aggr._end_time";
    static final String EVENT_TYPE = "event";
    static final String SUM_METRIC_DESCRIPTION = "Sum of the events";
    static final String SUM_METRIC_UNIT = "1";
    private final String key;
    private final String countKey;
    private final OutputFormat outputFormat;
    private final String metricName;

    @DataPrepperPluginConstructor
    public SumAggregateAction(final SumAggregateActionConfig sumAggregateActionConfig) {
        this.key = sumAggregateActionConfig.getKey();
        this.countKey = sumAggregateActionConfig.getCountKey();
        this.outputFormat = sumAggregateActionConfig.getOutputFormat();
        this.metricName = sumAggregateActionConfig.getMetricName();
    }

    public Exemplar createExemplar(final Event event, final double value) {
        final long curTimeNanos = getTimeNanos(Instant.now());
        final Map<String, Object> attributes = event.toMap();
        String spanId = null;
        String traceId = null;
        if (event instanceof Span) {
            final Span span = (Span) event;
            spanId = span.getSpanId();
            traceId = span.getTraceId();
        }
        return new DefaultExemplar(convertUnixNanosToISO8601(curTimeNanos), value, spanId, traceId, attributes);
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        final Number value = event.get(key, Number.class);
        if (value == null) {
            return AggregateActionResponse.nullEventResponse();
        }
        final double doubleValue = value.doubleValue();

        Instant eventStartTime = Instant.now();
        Instant eventEndTime = eventStartTime;
        final Object startTime = event.get(START_TIME_KEY, Object.class);
        final Object endTime = event.get(END_TIME_KEY, Object.class);
        if (startTime != null) {
            eventStartTime = AggregateProcessor.convertObjectToInstant(startTime);
        }
        if (endTime != null) {
            eventEndTime = AggregateProcessor.convertObjectToInstant(endTime);
        }

        if (groupState.get(SUM_KEY) == null) {
            groupState.putAll(aggregateActionInput.getIdentificationKeys());
            groupState.put(SUM_KEY, doubleValue);
            groupState.put(countKey, 1);
            groupState.put(EXEMPLAR_KEY, createExemplar(event, doubleValue));
            groupState.put(START_TIME_KEY, eventStartTime);
            groupState.put(END_TIME_KEY, eventEndTime);
        } else {
            final double sum = (double) groupState.get(SUM_KEY);
            groupState.put(SUM_KEY, sum + doubleValue);
            groupState.put(countKey, (Integer) groupState.get(countKey) + 1);
            final Instant groupStartTime = (Instant) groupState.get(START_TIME_KEY);
            final Instant groupEndTime = (Instant) groupState.get(END_TIME_KEY);
            if (eventStartTime.isBefore(groupStartTime)) {
                groupState.put(START_TIME_KEY, eventStartTime);
            }
            if (eventEndTime.isAfter(groupEndTime)) {
                groupState.put(END_TIME_KEY, eventEndTime);
            }
        }
        return AggregateActionResponse.nullEventResponse();
    }
    

    @Override
    public AggregateActionOutput concludeGroup(final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        if (groupState.isEmpty()) {
            return null;
        }

        Event event;
        final Instant startTime = (Instant) groupState.get(START_TIME_KEY);
        final Instant endTime = (Instant) groupState.get(END_TIME_KEY);
        final Exemplar exemplar = (Exemplar) groupState.remove(EXEMPLAR_KEY);
        groupState.remove(END_TIME_KEY);
        if (outputFormat == OutputFormat.RAW) {
            groupState.put(START_TIME_KEY, startTime.atZone(ZoneId.of(ZoneId.systemDefault().toString())).format(DateTimeFormatter.ofPattern(DATE_FORMAT)));
            event = JacksonEvent.builder()
                    .withEventType(EVENT_TYPE)
                    .withData(groupState)
                    .withEventHandle(aggregateActionInput.getEventHandle())
                    .build();
        } else {
            final double sumValue = (double) groupState.get(SUM_KEY);
            groupState.remove(SUM_KEY);
            groupState.remove(countKey);
            groupState.remove(START_TIME_KEY);
            final Map<String, Object> attr = new HashMap<>();
            groupState.forEach((k, v) -> attr.put((String) k, v));
            final JacksonSum sum = JacksonSum.builder()
                    .withName(metricName)
                    .withDescription(SUM_METRIC_DESCRIPTION)
                    .withTime(convertUnixNanosToISO8601(getTimeNanos(endTime)))
                    .withStartTime(convertUnixNanosToISO8601(getTimeNanos(startTime)))
                    .withIsMonotonic(false)
                    .withUnit(SUM_METRIC_UNIT)
                    .withAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA.name())
                    .withValue(sumValue)
                    .withExemplars(List.of(exemplar))
                    .withAttributes(attr)
                    .withEventHandle(aggregateActionInput.getEventHandle())
                    .build(false);
            event = (Event) sum;
        }

        return new AggregateActionOutput(List.of(event));
    }
}
