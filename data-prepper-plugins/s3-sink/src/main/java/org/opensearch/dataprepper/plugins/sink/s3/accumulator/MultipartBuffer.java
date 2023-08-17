/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.dataprepper.plugins.codec.parquet.S3OutputStream;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MultipartBuffer implements Buffer {

    private int eventCount;
    private final StopWatch watch;
    private boolean isCodecStarted;
    private S3OutputStream s3OutputStream;

    MultipartBuffer(S3OutputStream s3OutputStream) {
        this.s3OutputStream = s3OutputStream;
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
        isCodecStarted = false;
    }

    @Override
    public long getSize() {
        try {
            return s3OutputStream.getPos();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    public long getDuration() {
        return watch.getTime(TimeUnit.SECONDS);
    }

    /**
     * Upload accumulated data to s3 bucket.
     */
    @Override
    public void flushToS3() {
        s3OutputStream.close();
    }

    @Override
    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    @Override
    public String getKey() {
        return s3OutputStream.getKey();
    }

    @Override
    public S3OutputStream getOutputStream() {
        return s3OutputStream;
    }
}
