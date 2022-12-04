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
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action will add the unique keys of each smaller Event to the overall groupState,
 * and will create a combined Event from the groupState on concludeGroup. If smaller Events have the same keys, then these keys will be overwritten with the keys of the
 * most recently handled Event.
 * @since 2.1
 */
@DataPrepperPlugin(name = "count", pluginType = AggregateAction.class, pluginConfigurationType = CountAggregateActionConfig.class)
public class CountAggregateAction implements AggregateAction {
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
    public CountAggregateAction(final CountAggregateActionConfig countAggregateActionConfig) {
        this.countKey = countAggregateActionConfig.getCountKey();
        this.startTimeKey = countAggregateActionConfig.getStartTimeKey();
        this.outputFormat = countAggregateActionConfig.getOutputFormat();
    }

    private long getTimeNanos(Instant time) {
        final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
        long currentTimeNanos = time.getEpochSecond() * NANO_MULTIPLIER + time.getNano();
        return currentTimeNanos;
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        if (groupState.get(countKey) == null) {
            groupState.put(startTimeKey, Instant.now());
            groupState.putAll(aggregateActionInput.getIdentificationKeys());
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
        if (outputFormat == OutputFormat.RAW.toString()) {
            groupState.put(startTimeKey, startTime.atZone(ZoneId.of(ZoneId.systemDefault().toString())));
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
