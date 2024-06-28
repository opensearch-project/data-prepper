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
    private boolean isCodecStarted;


    public InMemoryBuffer(LambdaClient lambdaClient, String functionName, String invocationType) {
        this.lambdaClient = lambdaClient;
        this.functionName = functionName;
        this.invocationType = invocationType;

        byteArrayOutputStream.reset();
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
        isCodecStarted = false;
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
    public void flushToLambdaAsync() {
        InvokeResponse resp;
        SdkBytes payload = getPayload();

        // Setup an InvokeRequest.
        InvokeRequest request = InvokeRequest.builder()
                .functionName(functionName)
                .payload(payload)
                .invocationType(invocationType)
                .build();

        lambdaClient.invoke(request);
    }

    @Override
    public InvokeResponse flushToLambdaSync() {
        InvokeResponse resp;
        SdkBytes payload = getPayload();

        // Setup an InvokeRequest.
        InvokeRequest request = InvokeRequest.builder()
                .functionName(functionName)
                .payload(payload)
                .invocationType(invocationType)
                .build();

        resp = lambdaClient.invoke(request);
        return resp;
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
}

