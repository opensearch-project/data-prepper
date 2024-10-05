/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * A buffer can hold data before flushing it.
 */
public interface Buffer {

    long getSize();

    int getEventCount();

    Duration getDuration();

    CompletableFuture<InvokeResponse> flushToLambdaAsync(String invocationType);

    InvokeResponse flushToLambdaSync(String invocationType);

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

    void reset();

}
