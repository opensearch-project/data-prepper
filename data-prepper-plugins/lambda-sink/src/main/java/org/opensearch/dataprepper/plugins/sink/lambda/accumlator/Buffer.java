/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.lambda.accumlator;

import software.amazon.awssdk.core.SdkBytes;

import java.io.OutputStream;
import java.time.Duration;

/**
 * A buffer can hold data before flushing it.
 */
public interface Buffer {

    long getSize();

    int getEventCount();

    Duration getDuration();

    void flushToLambda();

    OutputStream getOutputStream();

    SdkBytes getPayload();

    void setEventCount(int eventCount);
}
