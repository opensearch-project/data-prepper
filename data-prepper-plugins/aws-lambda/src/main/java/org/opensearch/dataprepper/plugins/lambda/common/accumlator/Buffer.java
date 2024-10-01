/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

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

    InvokeResponse flushToLambdaAsync();

    InvokeResponse flushToLambdaSync();

    OutputStream getOutputStream();

    SdkBytes getPayload();

    void setEventCount(int eventCount);

    //Metrics
    public Duration getFlushLambdaSyncLatencyMetric();

    public Long getPayloadRequestSyncSize();

    public Duration getFlushLambdaAsyncLatencyMetric();

    public Long getPayloadResponseSyncSize();

    public Long getPayloadRequestAsyncSize();

    public Long getPayloadResponseAsyncSize();

}
