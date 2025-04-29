/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import javax.annotation.Nonnull;
import java.util.function.Consumer;

/**
 * A simple {@link Consumer} that pauses the current thread for a given number
 * of milliseconds.
 */
class ThreadSleeper implements Consumer<Integer> {

    /**
     * Sleeps for the specified duration in milliseconds.
     * If the thread is interrupted while sleeping, the interrupted status is cleared
     * and a {@link RuntimeException} is thrown to signal the failure.
     *
     * @param millis the number of milliseconds to sleep
     */
    @Override
    public void accept(@Nonnull final Integer millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
