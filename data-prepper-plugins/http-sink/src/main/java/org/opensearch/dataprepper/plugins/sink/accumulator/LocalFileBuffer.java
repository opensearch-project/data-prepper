/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hc.client5.http.classic.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;

import java.util.concurrent.TimeUnit;

/**
 * A buffer can hold local file data and flushing it to Http Endpoint.
 */
public class LocalFileBuffer implements Buffer {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileBuffer.class);
    private final OutputStream outputStream;
    private int eventCount;
    private final StopWatch watch;
    private final File localFile;

    LocalFileBuffer(final File tempFile) throws FileNotFoundException {
        localFile = tempFile;
        outputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
    }

    @Override
    public long getSize() {
        try {
            outputStream.flush();
        } catch (IOException e) {
            LOG.error("An exception occurred while flushing data to buffered output stream :", e);
        }
        return localFile.length();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    @Override
    public long getDuration(){
        return watch.getTime(TimeUnit.SECONDS);
    }

    /**
     * Upload accumulated data to http endpoint.
     * @param client HttpClient object.
     */
    @Override
    public void sendDataToHttpEndpoint(final HttpClient client) {
       //TODO : implement
    }

    /**
     * write byte array to output stream.
     * @param bytes byte array.
     * @throws IOException while writing to output stream fails.
     */
    @Override
    public void writeEvent(final byte[] bytes) throws IOException {

        //  TODO: implement
    }

}