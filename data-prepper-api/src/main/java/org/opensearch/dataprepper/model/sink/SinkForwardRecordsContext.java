/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.InternalEventHandle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SinkForwardRecordsContext {
    List<Record<Event>> records;
    boolean forwardPipelinesPresent;

    public SinkForwardRecordsContext(SinkContext sinkContext) {
        forwardPipelinesPresent =  (sinkContext != null && sinkContext.getForwardToPipelines().size() > 0);
        records = new ArrayList<>();
    }
    
    public void addRecord(Record<Event> record) {
        if (!forwardPipelinesPresent)
            return;
        InternalEventHandle eventHandle = (InternalEventHandle)record.getData().getEventHandle();
        if (eventHandle != null) {
            eventHandle.acquireReference();
        }
        records.add(record);
    }

    public void addRecords(Collection<Record<Event>> newRecords) {
        if (!forwardPipelinesPresent)
            return;
        newRecords.forEach((record) -> {
            Event event = record.getData();
            InternalEventHandle eventHandle = (InternalEventHandle)event.getEventHandle();
            if (eventHandle != null) {
                eventHandle.acquireReference();
            }
        });
        records.addAll(newRecords);
    }

    public List<Record<Event>> getRecords() {
        return records;
    }
}

