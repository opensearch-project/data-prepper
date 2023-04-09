/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.parser.DataFlowComponent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.pipeline.PipelineConnector;

import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.acknowledgements.InactiveAcknowledgementSetManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

public class RouterCopyRecordStrategy implements RouterGetRecordStrategy {
    private Set<Record> routedRecords;
    private Set<Record> referencedRecords;
    private AcknowledgementSetManager acknowledgementSetManager;
    private EventFactory eventFactory;

    public <C> RouterCopyRecordStrategy(final EventFactory eventFactory, final AcknowledgementSetManager acknowledgementSetManager, final Collection<DataFlowComponent<C>> dataFlowComponents) {
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.eventFactory = eventFactory;
        routedRecords = null;
        referencedRecords = new HashSet<Record>();
        /*
         * If there are more than one sink and one of the sinks is
         * pipeline connector, then we should make a copy of every
         * record that is routed to more than one sink, so, to keep
         * track of already routed records, initialize the set.
         */
        if (dataFlowComponents.size() > 1) {
            for (DataFlowComponent<C> dataFlowComponent : dataFlowComponents) {
                if (dataFlowComponent.getComponent() instanceof PipelineConnector) {
                    routedRecords = new HashSet<Record>();
                    break;
                }
            }
        }
    }

    Set<Record> getReferencedRecords() {
        return referencedRecords;
    }

    private void acquireEventReference(final Record record) {
        if (acknowledgementSetManager == InactiveAcknowledgementSetManager.getInstance() || record.getData() == null) {
            return;
        }
        if (referencedRecords.contains(record) || ((routedRecords != null) && routedRecords.contains(record))) {
            EventHandle eventHandle = ((JacksonEvent)record.getData()).getEventHandle();
            if (eventHandle != null) {
                acknowledgementSetManager.acquireEventReference(eventHandle);
            }
        } else if (!referencedRecords.contains(record)) {
            referencedRecords.add(record);
        }
    }
    @Override
    public Record getRecord(final Record record) {
        if (routedRecords == null) {
            acquireEventReference(record);
            return record;
        }
        if (!routedRecords.contains(record)) {
            acquireEventReference(record);
            routedRecords.add(record);
            return record;
        }
        if (record.getData() instanceof JacksonSpan) {
            // Not supporting acknowledgements for Span initially
            try {
                final Span spanEvent = (Span) record.getData();
                Span newSpanEvent = JacksonSpan.fromSpan(spanEvent);
                return new Record<>(newSpanEvent);
            } catch (Exception ex) {
            }
        } else if (record.getData() instanceof Event) {
            try {
                final Event recordEvent = (Event) record.getData();
                JacksonEvent newRecordEvent;
                Record newRecord;
                DefaultEventHandle eventHandle = (DefaultEventHandle)recordEvent.getEventHandle();
                if (eventHandle != null) {
                    final EventMetadata eventMetadata = recordEvent.getMetadata();
                    final EventBuilder eventBuilder = (EventBuilder) eventFactory.eventBuilder(EventBuilder.class).withEventMetadata(eventMetadata).withData(recordEvent.toMap());
                    newRecordEvent = (JacksonEvent) eventBuilder.build();

                    eventHandle.getAcknowledgementSet().add(newRecordEvent);
                    newRecord = new Record<>(newRecordEvent);
                    acquireEventReference(newRecord);
                } else {
                    // TODO we should have a way to create from factory
                    // even when acknowledgements are not used
                    newRecordEvent = JacksonEvent.fromEvent(recordEvent);
                    newRecord = new Record<>(newRecordEvent);
                }
                return newRecord;
            } catch (Exception ex) {
            }
        }
        return record;
    }

    @Override
    public Collection<Record> getAllRecords(final Collection<Record> allRecords) {
        if (routedRecords == null) {
            allRecords.stream().forEach((record) -> acquireEventReference(record));
            return allRecords;
        }
        if (routedRecords.isEmpty()) {
            allRecords.stream().forEach((record) -> acquireEventReference(record));
            routedRecords.addAll(allRecords);
            return allRecords;
        }
        List<Record> newRecords = new ArrayList<Record>();
        for (Record record : allRecords) {
            newRecords.add(getRecord(record));
        }
        return newRecords;
    }

}
