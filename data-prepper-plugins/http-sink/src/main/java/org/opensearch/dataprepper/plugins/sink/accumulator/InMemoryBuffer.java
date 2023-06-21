/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hc.client5.http.classic.HttpClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A buffer can hold in memory data and flushing it to Http Endpoint.
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
     * Upload accumulated data to http endpoint.
     * @param client HttpClient object.
     */
    @Override
    public void sendDataToHttpEndpoint(final HttpClient client) {
       //TODO: implement
    }

    /**
     * write byte array to output stream.
     *
     * @param bytes byte array.
     * @throws IOException while writing to output stream fails.
     */
    @Override
    public void writeEvent(final byte[] bytes) throws IOException {
       //TODO: implement
    }
}