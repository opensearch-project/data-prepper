/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.common.lambda.accumlator;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.OutputStream;
import java.time.Duration;

/**
 * A buffer can hold data before flushing it.
 */
public interface Buffer {

    long getSize();

    int getEventCount();

    Duration getDuration();

    void flushToLambdaAsync();

    InvokeResponse flushToLambdaSync();

    OutputStream getOutputStream();

    SdkBytes getPayload();

    void setEventCount(int eventCount);
}
