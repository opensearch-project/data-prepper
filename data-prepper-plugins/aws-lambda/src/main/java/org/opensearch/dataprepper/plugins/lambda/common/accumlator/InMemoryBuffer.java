/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.time.StopWatch;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * A buffer can hold in memory data and flushing it.
 */
public class InMemoryBuffer implements Buffer {

    private static final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    private final LambdaClient lambdaClient;
    private final String functionName;
    private final String invocationType;
    private int eventCount;
    private final StopWatch watch;
    private final StopWatch lambdaSyncLatencyWatch;
    private final StopWatch lambdaAsyncLatencyWatch;
    private boolean isCodecStarted;
    private long payloadRequestSyncSize;
    private long payloadResponseSyncSize;
    private long payloadRequestAsyncSize;
    private long payloadResponseAsyncSize;


    public InMemoryBuffer(LambdaClient lambdaClient, String functionName, String invocationType) {
        this.lambdaClient = lambdaClient;
        this.functionName = functionName;
        this.invocationType = invocationType;

        byteArrayOutputStream.reset();
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
        isCodecStarted = false;
        lambdaSyncLatencyWatch = new StopWatch();
        lambdaAsyncLatencyWatch = new StopWatch();
        payloadRequestSyncSize = 0;
        payloadResponseSyncSize = 0;
        payloadRequestAsyncSize = 0;
        payloadResponseAsyncSize =0;
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
        return Duration.ofMillis(watch.getTime(TimeUnit.MILLISECONDS));
    }


    @Override
    public InvokeResponse flushToLambdaAsync() {
        InvokeResponse resp;
        SdkBytes payload = getPayload();
        payloadRequestAsyncSize = payload.asByteArray().length;

        // Setup an InvokeRequest.
        InvokeRequest request = InvokeRequest.builder()
                .functionName(functionName)
                .payload(payload)
                .invocationType(invocationType)
                .build();

        lambdaAsyncLatencyWatch.start();
        resp = lambdaClient.invoke(request);
        lambdaAsyncLatencyWatch.stop();
        payloadResponseAsyncSize = resp.payload().asByteArray().length;
        return resp;
    }

    @Override
    public InvokeResponse flushToLambdaSync() {
        InvokeResponse resp = null;
        SdkBytes payload = getPayload();
        payloadRequestSyncSize = payload.asByteArray().length;

        // Setup an InvokeRequest.
        InvokeRequest request = InvokeRequest.builder()
                .functionName(functionName)
                .payload(payload)
                .invocationType(invocationType)
                .build();

        lambdaSyncLatencyWatch.start();
        try {
            resp = lambdaClient.invoke(request);
            payloadResponseSyncSize = resp.payload().asByteArray().length;
            lambdaSyncLatencyWatch.stop();
            return resp;
        } catch (LambdaException e){
            lambdaSyncLatencyWatch.stop();
            throw new RuntimeException(e);
        }
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

    public Duration getFlushLambdaSyncLatencyMetric (){
        return Duration.ofMillis(lambdaSyncLatencyWatch.getTime(TimeUnit.MILLISECONDS));
    }

    public Duration getFlushLambdaAsyncLatencyMetric (){
        return Duration.ofMillis(lambdaAsyncLatencyWatch.getTime(TimeUnit.MILLISECONDS));
    }

    public Long getPayloadRequestSyncSize() {
        return payloadRequestSyncSize;
    }

    public Long getPayloadResponseSyncSize() {
        return payloadResponseSyncSize;
    }

    public Long getPayloadRequestAsyncSize() {
        return payloadRequestAsyncSize;
    }

    public Long getPayloadResponseAsyncSize() {
        return payloadResponseAsyncSize;
    }
}

