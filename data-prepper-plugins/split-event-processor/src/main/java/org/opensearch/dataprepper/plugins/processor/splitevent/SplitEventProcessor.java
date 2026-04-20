/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.splitevent;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;


@DataPrepperPlugin(name = "split_event", pluginType = Processor.class, pluginConfigurationType = SplitEventProcessorConfig.class)
public class SplitEventProcessor extends AbstractProcessor<Record<Event>, Record<Event>>{
    final String delimiter;
    final String delimiterRegex;
    final String field;
    final Pattern pattern;
    final boolean arrayMode;
    private final Function<String, String[]> splitter;

    @DataPrepperPluginConstructor
    public SplitEventProcessor(final PluginMetrics pluginMetrics, final SplitEventProcessorConfig config) {
        super(pluginMetrics);
        this.delimiter = config.getDelimiter();
        this.delimiterRegex = config.getDelimiterRegex();
        this.field = config.getField();

        if(delimiterRegex != null && !delimiterRegex.isEmpty()
                && delimiter != null && !delimiter.isEmpty()) {
            throw new IllegalArgumentException("delimiter and delimiter_regex cannot be defined at the same time");
        }

        boolean hasDelimiter = (delimiter != null && !delimiter.isEmpty());
        boolean hasRegex = (delimiterRegex != null && !delimiterRegex.isEmpty());

        if (hasRegex) {
            pattern = Pattern.compile(delimiterRegex);
            splitter = pattern::split;
            arrayMode = false;
        } else if (hasDelimiter) {
            splitter = inputString -> inputString.split(delimiter);
            pattern = null;
            arrayMode = false;
        } else {
            splitter = null;
            pattern = null;
            arrayMode = true;
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        Collection<Record<Event>> newRecords = new ArrayList<>();
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            if (!recordEvent.containsKey(field)) {
                newRecords.add(record);
                continue;
            }

            final Object value = recordEvent.get(field, Object.class);

            if (arrayMode) {
                if (value instanceof List) {
                    splitArrayField(record, recordEvent, (List<?>) value, newRecords);
                } else {
                    newRecords.add(record);
                }
                continue;
            }

            if (!(value instanceof String)) {
                newRecords.add(record);
                continue;
            }

            final String[] splitValues = splitter.apply((String) value);

            if(splitValues.length <= 1) {
                newRecords.add(record);
                continue;
            }

            splitIntoRecords(record, recordEvent, splitValues, newRecords);
        }
        return newRecords;
    }

    private void splitArrayField(final Record<Event> record, final Event recordEvent,
                                 final List<?> arrayValue, final Collection<Record<Event>> newRecords) {
        if (arrayValue.size() <= 1) {
            if (arrayValue.size() == 1) {
                recordEvent.put(field, arrayValue.get(0));
            }
            newRecords.add(record);
            return;
        }

        splitIntoRecords(record, recordEvent, arrayValue.toArray(), newRecords);
    }

    private void splitIntoRecords(final Record<Event> record, final Event recordEvent,
                                  final Object[] values, final Collection<Record<Event>> newRecords) {
        for (int i = 0; i < values.length - 1; i++) {
            Record newRecord = createNewRecordFromEvent(recordEvent, values[i]);
            addToAcknowledgementSetFromOriginEvent((Event) newRecord.getData(), recordEvent);
            newRecords.add(newRecord);
        }

        recordEvent.put(field, values[values.length - 1]);
        newRecords.add(record);
    }

    protected Record createNewRecordFromEvent(final Event recordEvent, Object splitValue) {
        Record newRecord;
        JacksonEvent newRecordEvent;

        newRecordEvent = JacksonEvent.fromEvent(recordEvent);
        newRecordEvent.put(field, splitValue);
        newRecord = new Record<>(newRecordEvent);
        return newRecord;
    }

    protected void addToAcknowledgementSetFromOriginEvent(Event recordEvent, Event originRecordEvent) {
        DefaultEventHandle eventHandle = (DefaultEventHandle) originRecordEvent.getEventHandle();
        if (eventHandle != null) {
            eventHandle.addEventHandle(recordEvent.getEventHandle());
        }
    }

    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}
