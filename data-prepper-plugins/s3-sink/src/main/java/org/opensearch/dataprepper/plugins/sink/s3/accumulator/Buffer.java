/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;


import java.io.OutputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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

    Duration getDuration();

    Optional<CompletableFuture<?>> flushToS3(final Consumer<Boolean> consumeOnGroupCompletion, final Consumer<Throwable> runOnFailure);

    OutputStream getOutputStream();

    void setEventCount(int eventCount);

    String getKey();
}
