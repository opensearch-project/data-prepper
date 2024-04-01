/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;

import java.util.Collection;
import java.util.LinkedList;

public class S3Group implements Comparable<S3Group> {

    private final Buffer buffer;

    private final S3GroupIdentifier s3GroupIdentifier;

    private final Collection<EventHandle> groupEventHandles;

    public S3Group(final S3GroupIdentifier s3GroupIdentifier,
                   final Buffer buffer) {
        this.buffer = buffer;
        this.s3GroupIdentifier = s3GroupIdentifier;
        this.groupEventHandles = new LinkedList<>();
    }

    public Buffer getBuffer() {
        return buffer;
    }

    S3GroupIdentifier getS3GroupIdentifier() { return s3GroupIdentifier; }

    public void addEventHandle(final EventHandle eventHandle) {
        groupEventHandles.add(eventHandle);
    }

    public void releaseEventHandles(final boolean result) {
        for (EventHandle eventHandle : groupEventHandles) {
            eventHandle.release(result);
        }

        groupEventHandles.clear();
    }

    @Override
    public int compareTo(final S3Group o) {
        return Long.compare(o.getBuffer().getSize(), buffer.getSize());
    }
}
