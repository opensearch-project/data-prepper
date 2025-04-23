/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

import javax.annotation.Nonnull;
import java.util.function.Consumer;

class ThreadSleeper implements Consumer<Integer> {

    /**
     * Sleeps for the specified duration in milliseconds.
     * If the thread is interrupted while sleeping, the interrupted status is cleared.
     *
     * @param millis the input argument
     */
    @Override
    public void accept(final @Nonnull Integer millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
