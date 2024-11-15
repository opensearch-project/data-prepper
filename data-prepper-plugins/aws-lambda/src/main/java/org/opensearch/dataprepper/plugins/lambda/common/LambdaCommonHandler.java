package org.opensearch.dataprepper.plugins.lambda.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LambdaCommonHandler {
    private static final Logger LOG = LoggerFactory.getLogger(LambdaCommonHandler.class);

    public static boolean checkStatusCode(InvokeResponse response) {
        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            LOG.error("Lambda invocation returned with non-success status code: {}", statusCode);
            return false;
        }
        return true;
    }

    public static void waitForFutures(List<CompletableFuture<Void>> futureList) {
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