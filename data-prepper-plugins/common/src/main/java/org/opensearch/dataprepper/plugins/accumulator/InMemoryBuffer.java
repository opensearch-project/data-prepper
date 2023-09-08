/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

/**
 * A buffer can hold in memory data and flushing it to any Sink.
 */
public class InMemoryBuffer implements Buffer {

    private static final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private int eventCount;
    private final StopWatch watch;

    InMemoryBuffer() {
        byteArrayOutputStream.reset();
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
    }

    @Override
    public long getSize() {
        return byteArrayOutputStream.size();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    public long getDuration() {
        return watch.getTime(TimeUnit.SECONDS);
    }

    /**
     * collect current buffer data.
     * @throws IOException while collecting current buffer data.
     */
    @Override
    public byte[] getSinkBufferData() throws IOException {
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public OutputStream getOutputStream() {
        return byteArrayOutputStream;
    }

    @Override
    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }
}