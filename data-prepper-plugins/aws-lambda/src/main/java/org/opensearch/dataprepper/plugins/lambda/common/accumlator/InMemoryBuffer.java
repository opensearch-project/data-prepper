/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

/**
 * A buffer can hold in memory data and flushing it.
 * NOT Thread-safe by default
 */
public class InMemoryBuffer implements Buffer {

    private final ByteArrayOutputStream byteArrayOutputStream;

    protected List<Record<Event>> records;
    private final StopWatch bufferWatch;
    private final StopWatch lambdaLatencyWatch;
    private final OutputCodec requestCodec;
    private final OutputCodecContext outputCodecContext;
    private final long payloadResponseSize;
    private int eventCount;
    private long payloadRequestSize;


    public InMemoryBuffer(String batchOptionKeyName) {
        this(batchOptionKeyName, new OutputCodecContext());
    }

    public InMemoryBuffer(String batchOptionKeyName, OutputCodecContext outputCodecContext) {
        byteArrayOutputStream = new ByteArrayOutputStream(32 * 1024);
        records = new ArrayList<>();
        bufferWatch = new StopWatch();
        bufferWatch.start();
        lambdaLatencyWatch = new StopWatch();
        eventCount = 0;
        payloadRequestSize = 0;
        payloadResponseSize = 0;
        // Setup request codec
        JsonOutputCodecConfig jsonOutputCodecConfig = new JsonOutputCodecConfig();
        jsonOutputCodecConfig.setKeyName(batchOptionKeyName);
        requestCodec = new JsonOutputCodec(jsonOutputCodecConfig);
        this.outputCodecContext = outputCodecContext;
    }

    @Override
    public void addRecord(Record<Event> record) {
        records.add(record);
        Event event = record.getData();
        try {
            if (eventCount == 0) {
                requestCodec.start(this.byteArrayOutputStream, event, this.outputCodecContext);
            }
            requestCodec.writeEvent(event, this.byteArrayOutputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        eventCount++;
    }

    @Override
    public List<Record<Event>> getRecords() {
        return records;
    }

    @Override
    public long getSize() {
        return byteArrayOutputStream.size();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    public Duration getDuration() {
        return Duration.ofMillis(bufferWatch.getTime(TimeUnit.MILLISECONDS));
    }

    @Override
    public InvokeRequest getRequestPayload(String functionName, String invocationType) {

        if (eventCount == 0) {
            //We never added any events so there is no payload
            return null;
        }

        try {
            requestCodec.complete(this.byteArrayOutputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SdkBytes payload = getPayload();
        payloadRequestSize = payload.asByteArrayUnsafe().length;

        // Setup an InvokeRequest.
        InvokeRequest request = InvokeRequest.builder()
                .functionName(functionName)
                .payload(payload)
                .invocationType(invocationType)
                .build();

        synchronized (this) {
            if (lambdaLatencyWatch.isStarted()) {
                lambdaLatencyWatch.reset();
            }
            lambdaLatencyWatch.start();
        }
        return request;
    }

    public synchronized Duration stopLatencyWatch() {
        if (lambdaLatencyWatch.isStarted()) {
            lambdaLatencyWatch.stop();
        }
        long timeInMillis = lambdaLatencyWatch.getTime();
        return Duration.ofMillis(timeInMillis);
    }

    private SdkBytes getPayload() {
        return SdkBytes.fromByteArrayUnsafe(byteArrayOutputStream.toByteArray());
    }

    public Duration getFlushLambdaLatencyMetric() {
        return Duration.ofMillis(lambdaLatencyWatch.getTime(TimeUnit.MILLISECONDS));
    }

    public Long getPayloadRequestSize() {
        return payloadRequestSize;
    }
}

