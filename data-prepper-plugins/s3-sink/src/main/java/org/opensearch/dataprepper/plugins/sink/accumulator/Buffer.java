/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import software.amazon.awssdk.services.s3.S3Client;
import java.io.IOException;

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

    void flushToS3(S3Client s3Client, String bucket, String key) ;
    void writeEvent(byte[] bytes) throws IOException;
}
