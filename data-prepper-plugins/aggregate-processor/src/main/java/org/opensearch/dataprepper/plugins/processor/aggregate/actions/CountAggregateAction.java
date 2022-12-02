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
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action will add the unique keys of each smaller Event to the overall groupState,
 * and will create a combined Event from the groupState on concludeGroup. If smaller Events have the same keys, then these keys will be overwritten with the keys of the
 * most recently handled Event.
 * @since 1.3
 */
@DataPrepperPlugin(name = "count", pluginType = AggregateAction.class, pluginConfigurationType = CountAggregateActionConfig.class)
public class CountAggregateAction implements AggregateAction {
    static final String EVENT_TYPE = "event";
    public final String countKey;
    public final String outputFormat;
    private long startTimeNanos;

    @DataPrepperPluginConstructor
    public CountAggregateAction(final CountAggregateActionConfig countAggregateActionConfig) {
        this.countKey = countAggregateActionConfig.getCountKey();
        this.outputFormat = countAggregateActionConfig.getOutputFormat();
    }

    private long getCurrentTimeNanos() {
        final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
        Instant now = Instant.now();
        long currentTimeNanos = now.getEpochSecond() * NANO_MULTIPLIER + now.getNano();
        return currentTimeNanos;
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        if (groupState.get(countKey) == null) {
            startTimeNanos = getCurrentTimeNanos();
            groupState.putAll(aggregateActionInput.getIdentificationKeysMap());
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
        if (outputFormat == OutputFormat.DEFAULT.toString()) {
            event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(groupState)
                .build();
        } else {
            Integer countValue = (Integer)groupState.get(countKey);
            groupState.remove(countKey);
            long currentTimeNanos = getCurrentTimeNanos();
            Map<String, Object> attr = new HashMap<String, Object>();
            groupState.forEach((k, v) -> attr.put((String)k, v));
            JacksonSum js = JacksonSum.builder()
                .withName("count")
                .withDescription("Number of events")
                .withTime(OTelProtoCodec.convertUnixNanosToISO8601(currentTimeNanos))
                .withStartTime(OTelProtoCodec.convertUnixNanosToISO8601(startTimeNanos))
                .withIsMonotonic(true)
                .withUnit("1")
                .withAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE.name())
                .withValue((double)countValue)
                .withAttributes(attr)
                .build();
            event = (Event)js;
        }
        
        return Optional.of(event);
    }
}
