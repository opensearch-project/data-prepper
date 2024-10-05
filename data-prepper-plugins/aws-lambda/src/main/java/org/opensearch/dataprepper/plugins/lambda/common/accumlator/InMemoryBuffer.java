/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumlator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.time.StopWatch;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * A buffer can hold in memory data and flushing it.
 */
public class InMemoryBuffer implements Buffer {

    private static final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    private final LambdaAsyncClient lambdaAsyncClient;
    private final String functionName;
    private final String invocationType;
    private int eventCount;
    private StopWatch watch;
    private StopWatch lambdaSyncLatencyWatch;
    private StopWatch lambdaAsyncLatencyWatch;
    private boolean isCodecStarted;
    private long payloadRequestSyncSize;
    private long payloadResponseSyncSize;
    private long payloadRequestAsyncSize;
    private long payloadResponseAsyncSize;


    public InMemoryBuffer(LambdaAsyncClient lambdaAsyncClient, String functionName, String invocationType) {
        this.lambdaAsyncClient = lambdaAsyncClient;
        this.functionName = functionName;
        this.invocationType = invocationType;

        byteArrayOutputStream.reset();
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
        lambdaSyncLatencyWatch = new StopWatch();
        lambdaAsyncLatencyWatch = new StopWatch();
        isCodecStarted = false;
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

    public void reset() {
        byteArrayOutputStream.reset();
        eventCount = 0;
        watch.reset();
        watch.start();
        lambdaSyncLatencyWatch.reset();
        lambdaAsyncLatencyWatch.reset();
        isCodecStarted = false;
        payloadRequestSyncSize = 0;
        payloadResponseSyncSize = 0;
        payloadRequestAsyncSize = 0;
        payloadResponseAsyncSize = 0;
    }

    @Override
    public CompletableFuture<InvokeResponse> flushToLambdaAsync(String invocationType) {
        SdkBytes payload = getPayload();
        payloadRequestAsyncSize = payload.asByteArray().length;

        // Setup an InvokeRequest.
        InvokeRequest request = InvokeRequest.builder()
                .functionName(functionName)
                .payload(payload)
                .invocationType(invocationType)
                .build();

        if (lambdaAsyncLatencyWatch.isStarted()) {
            lambdaAsyncLatencyWatch.reset();
        }
        lambdaAsyncLatencyWatch.start();
        // Use the async client to invoke the Lambda function
        CompletableFuture<InvokeResponse> future = lambdaAsyncClient.invoke(request);

        // When the future completes, stop the latency watch and set payload size
        future = future.handle((resp, throwable) -> {
            lambdaAsyncLatencyWatch.stop();
            if (throwable == null) {
                payloadResponseAsyncSize = resp.payload().asByteArray().length;
                return resp;
            } else {
                // Rethrow the exception to propagate it
                throw new CompletionException(throwable);
            }
        });
        return future;
    }

    @Override
    @Deprecated
    public InvokeResponse flushToLambdaSync(String invocationType) {
        return null;
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

