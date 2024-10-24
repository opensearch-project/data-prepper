/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A buffer can hold in memory data and flushing it.
 */
public class InMemoryBuffer implements Buffer {

    private final ByteArrayOutputStream byteArrayOutputStream;

    private final LambdaAsyncClient lambdaAsyncClient;
    private final String functionName;
    private final InvocationType invocationType;
    private int eventCount;
    private StopWatch bufferWatch;
    private StopWatch lambdaLatencyWatch;
    private long payloadRequestSize;
    private long payloadResponseSize;
    private boolean isCodecStarted;
    private final List<Record<Event>> records;


    public InMemoryBuffer(LambdaAsyncClient lambdaAsyncClient, String functionName, InvocationType invocationType) {
        this.lambdaAsyncClient = lambdaAsyncClient;
        this.functionName = functionName;
        this.invocationType = invocationType;
        byteArrayOutputStream = new ByteArrayOutputStream();
        records = new ArrayList<>();
        bufferWatch = new StopWatch();
        bufferWatch.start();
        lambdaLatencyWatch = new StopWatch();
        eventCount = 0;
        isCodecStarted = false;
        payloadRequestSize = 0;
        payloadResponseSize = 0;
    }

    public void addRecord(Record<Event> record) {
        records.add(record);
        eventCount++;
    }

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

    public void reset() {
        byteArrayOutputStream.reset();
        eventCount = 0;
        bufferWatch.reset();
        lambdaLatencyWatch.reset();
        isCodecStarted = false;
        payloadRequestSize = 0;
        payloadResponseSize = 0;
    }

    @Override
    public CompletableFuture<InvokeResponse> flushToLambda(InvocationType invocationType) {
        SdkBytes payload = getPayload();
        payloadRequestSize = payload.asByteArray().length;

        // Setup an InvokeRequest.
        InvokeRequest request = InvokeRequest.builder()
                .functionName(functionName)
                .payload(payload)
                .invocationType(invocationType.getAwsLambdaValue())
                .build();

        synchronized (this) {
            if (lambdaLatencyWatch.isStarted()) {
                lambdaLatencyWatch.reset();
            }
            lambdaLatencyWatch.start();
        }
        // Use the async client to invoke the Lambda function
        CompletableFuture<InvokeResponse> future = lambdaAsyncClient.invoke(request);
        return future;
    }

    public synchronized Duration stopLatencyWatch() {
        if (lambdaLatencyWatch.isStarted()) {
            lambdaLatencyWatch.stop();
        }
        long timeInMillis = lambdaLatencyWatch.getTime();
        return Duration.ofMillis(timeInMillis);
    }

    private SdkBytes validatePayload(String payload_string) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode jsonNode = mapper.readTree(byteArrayOutputStream.toByteArray());

            // Convert the JsonNode back to a String to normalize it (removes extra spaces, trailing commas, etc.)
            String normalizedJson = mapper.writeValueAsString(jsonNode);
            return SdkBytes.fromUtf8String(normalizedJson);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    @Override
    public OutputStream getOutputStream() {
        return byteArrayOutputStream;
    }

    @Override
    public SdkBytes getPayload() {
        byte[] bytes = byteArrayOutputStream.toByteArray();
        SdkBytes sdkBytes = SdkBytes.fromByteArray(bytes);
        return sdkBytes;
    }

    public Duration getFlushLambdaLatencyMetric (){
        return Duration.ofMillis(lambdaLatencyWatch.getTime(TimeUnit.MILLISECONDS));
    }

    public Long getPayloadRequestSize() {
        return payloadRequestSize;
    }

    public Long getPayloadResponseSize() {
        return payloadResponseSize;
    }

    public StopWatch getBufferWatch() {return bufferWatch;}

    public StopWatch getLambdaLatencyWatch(){return lambdaLatencyWatch;}

}

