/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import software.amazon.awssdk.services.s3.S3Client;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A buffer can hold data before flushing it to S3.
 */
public interface Buffer {

    /**
     * Gets the current size of the buffer. This should be the number of bytes.
     * @return buffer size.
     */
    long getSize();
    int getEventCount();

    long getDuration();
    boolean isCodecStarted();
    void setCodecStarted(boolean codecStarted);

    void flushToS3(S3Client s3Client, String bucket, String key) ;
    void writeEvent(byte[] bytes) throws IOException;
    OutputStream getOutputStream();

    void setEventCount(int eventCount);
}
