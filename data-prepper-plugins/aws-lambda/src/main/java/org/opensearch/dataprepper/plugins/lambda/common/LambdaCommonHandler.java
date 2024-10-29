package org.opensearch.dataprepper.plugins.lambda.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.slf4j.Logger;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LambdaCommonHandler {
    private final Logger LOG;
    private final LambdaAsyncClient lambdaAsyncClient;
    private final String functionName;
    private final String invocationType;
    BufferFactory bufferFactory;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LambdaCommonHandler(
            final Logger log,
            final LambdaAsyncClient lambdaAsyncClient,
            final String functionName,
            final String invocationType,
            BufferFactory bufferFactory){
        this.LOG = log;
        this.lambdaAsyncClient = lambdaAsyncClient;
        this.functionName = functionName;
        this.invocationType = invocationType;
        this.bufferFactory = bufferFactory;
    }

    public Buffer createBuffer(Buffer currentBuffer) {
        try {
            LOG.debug("Resetting buffer");
            currentBuffer = bufferFactory.getBuffer(lambdaAsyncClient, functionName, invocationType);
            return currentBuffer;
        } catch (IOException e) {
            throw new RuntimeException("Failed to reset buffer", e);
        }
    }

    public boolean checkStatusCode(InvokeResponse response) {
        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            LOG.error("Lambda invocation returned with non-success status code: {}", statusCode);
            return false;
        }
        return true;
    }

    public void waitForFutures(List<CompletableFuture<Void>> futureList) {
        if (!futureList.isEmpty()) {
            try {
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
                LOG.info("All {} Lambda invocations have completed", futureList.size());
            } catch (Exception e) {
                LOG.warn("Exception while waiting for Lambda invocations to complete", e);
            } finally {
                futureList.clear();
            }
        }
    }
}
