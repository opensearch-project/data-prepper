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
import java.util.function.Function;
import org.opensearch.dataprepper.model.pattern.Pattern;


@DataPrepperPlugin(name = "split_event", pluginType = Processor.class, pluginConfigurationType = SplitEventProcessorConfig.class)
public class SplitEventProcessor extends AbstractProcessor<Record<Event>, Record<Event>>{
    final String delimiter;
    final String delimiterRegex;
    final String field;
    final Pattern pattern;
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
        } else if((delimiterRegex == null || delimiterRegex.isEmpty()) &&
                (delimiter == null || delimiter.isEmpty())) {
            throw new IllegalArgumentException("delimiter or delimiter_regex needs to be defined");
        }

        if(delimiterRegex != null && !delimiterRegex.isEmpty()) {
            pattern = Pattern.compile(delimiterRegex);
            splitter = pattern::split;
        } else {
            splitter = inputString -> inputString.split(delimiter);
            pattern = null;
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

            //split record according to delimiter
            final String[] splitValues = splitter.apply((String) value);

           // when no splits or empty value use the original record
           if(splitValues.length <= 1) {
                newRecords.add(record);
                continue;
           }

            //create new events for the splits 
            for (int i = 0; i < splitValues.length-1 ; i++) {
                Record newRecord = createNewRecordFromEvent(recordEvent, splitValues[i]);
                addToAcknowledgementSetFromOriginEvent((Event) newRecord.getData(), recordEvent);
                newRecords.add(newRecord);
            }

            // Modify original event to hold the last split
            recordEvent.put(field, splitValues[splitValues.length-1]);
            newRecords.add(record);
        }
        return newRecords;
    }

    protected Record createNewRecordFromEvent(final Event recordEvent, String splitValue) {
        Record newRecord;
        JacksonEvent newRecordEvent;

        newRecordEvent = JacksonEvent.fromEvent(recordEvent);
        newRecordEvent.put(field,(Object) splitValue);
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
