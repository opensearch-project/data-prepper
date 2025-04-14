/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.http;

/**
 * Interface for sleeping in tests.
 */
interface Sleeper {

    /**
     * Sleeps for the specified number of milliseconds.
     *
     * @param millis the number of milliseconds to sleep
     * @throws InterruptedException if the sleep is interrupted
     */
    void sleep(int millis) throws InterruptedException;
}