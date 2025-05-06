/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.accumulator.Buffer;

import java.util.ArrayList;
import java.util.List;

public class SqsSinkBatchEntry {
    private final List<EventHandle> eventHandles;
    private final String groupId;
    private final String deDupId;
    private final Buffer buffer;
    private final OutputCodec codec;
    private final OutputCodecContext codecContext;
    private OutputCodec.Writer writer;
    private int eventCount;
    private int size;
    private boolean completed;

    public SqsSinkBatchEntry(final Buffer buffer, final String groupId, final String deDupId, final OutputCodec codec, final OutputCodecContext codecContext) {
        this.eventHandles = new ArrayList<>();
        this.buffer = buffer;
        completed = false;
        this.groupId = groupId;
        this.deDupId = deDupId;
        this.codec = codec;
        this.codecContext = codecContext;
        
        this.eventCount = 0;
        size = 0;
    }

    public String getBody() {
        return buffer.getOutputStream().toString();
    }

    public void releaseEventHandles(boolean result) {
        for (EventHandle eventHandle: eventHandles) {
            eventHandle.release(result);
        }
    }

    public void addEvent(final Event event) throws Exception {
        if (completed) {
            throw new RuntimeException("Batch is completed");
        }
        if (eventCount == 0) {
            writer = codec.createWriter(buffer.getOutputStream(), null, codecContext);
        }
        writer.writeEvent(event);
        eventHandles.add(event.getEventHandle());
        eventCount++;
    }

    public long getSize() {
        return buffer.getSize();
    }

    public void complete() throws Exception {
        if (completed) {
            return;
        }
        writer.complete();
        completed = true;
    }


    public String getGroupId() {
        return groupId;
    }

    public String getDedupId() {
        return deDupId;
    }

    public List<EventHandle> getEventHandles() {
        return eventHandles;
    }

    public int getEventCount() {
        return eventCount;
    }

}

