/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A buffer can hold data before flushing it.
 */
public interface Buffer {

    long getSize();

    int getEventCount();

    Duration getDuration();

    CompletableFuture<InvokeResponse> flushToLambda(String invocationType);

    OutputStream getOutputStream();

    SdkBytes getPayload();

    void setEventCount(int eventCount);

    public void addRecord(Record<Event> record);

    public List<Record<Event>> getRecords();

    public Duration getFlushLambdaLatencyMetric();

    public Long getPayloadRequestSize();

    public Duration stopLatencyWatch();

    void reset();

}
