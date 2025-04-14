/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

/**
 * Implementation of {@link Sleeper} that sleeps using {@link Thread#sleep(long)}.
 */
class ThreadSleeper implements Sleeper {
    @Override
    public void sleep(int millis) throws InterruptedException {
        Thread.sleep(millis);
    }
}
