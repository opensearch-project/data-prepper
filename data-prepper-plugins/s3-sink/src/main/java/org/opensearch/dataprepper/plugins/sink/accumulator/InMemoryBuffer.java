/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import java.util.NavigableSet;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Implements the in memory buffer type.
 */
public class InMemoryBuffer implements BufferType {

    private S3Client s3Client;
    private S3SinkConfig s3SinkConfig;

    public InMemoryBuffer() {
    }

    /**
     * @param s3Client
     * @param s3SinkConfig
     */
    public InMemoryBuffer(final S3Client s3Client, final S3SinkConfig s3SinkConfig) {
        this.s3Client = s3Client;
        this.s3SinkConfig = s3SinkConfig;
    }

    /**
     * @param bufferedEventSet
     * @return boolean
     * @throws InterruptedException
     */
    public boolean inMemoryAccumulate(final NavigableSet<String> bufferedEventSet) throws InterruptedException {

        StringBuilder eventBuilder = new StringBuilder();
        for (String event : bufferedEventSet) {
            eventBuilder.append(event);
        }
        return uploadToAmazonS3(s3SinkConfig, s3Client, RequestBody.fromString(eventBuilder.toString()));
    }
}