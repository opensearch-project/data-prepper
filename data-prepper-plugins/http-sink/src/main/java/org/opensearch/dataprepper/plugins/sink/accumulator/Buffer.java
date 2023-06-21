/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import org.apache.hc.client5.http.classic.HttpClient;

import java.io.IOException;

/**
 * A buffer can hold data before flushing it to Http Endpoint.
 */
public interface Buffer {

    /**
     * Gets the current size of the buffer. This should be the number of bytes.
     * @return buffer size.
     */
    long getSize();
    int getEventCount();

    long getDuration();

    void sendDataToHttpEndpoint(HttpClient client) ;

    void writeEvent(byte[] bytes) throws IOException;
}
