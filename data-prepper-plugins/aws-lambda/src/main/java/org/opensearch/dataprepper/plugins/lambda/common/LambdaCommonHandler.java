package org.opensearch.dataprepper.plugins.lambda.common;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LambdaCommonHandler {
    private static final Logger LOG = LoggerFactory.getLogger(LambdaCommonHandler.class);

    private LambdaCommonHandler() {
    }

    public static boolean checkStatusCode(InvokeResponse response) {
        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            LOG.error("Lambda invocation returned with non-success status code: {}", statusCode);
            return false;
        }
        return true;
    }

    public static void waitForFutures(List<CompletableFuture<InvokeResponse>> futureList) {
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

    public static List<Buffer> createBufferBatches(Collection<Record<Event>> records,
                                                   String keyName,
                                                   String whenCondition,
                                                   ExpressionEvaluator expressionEvaluator,
                                                   int maxEvents,
                                                   ByteCount maxBytes,
                                                   Duration maxCollectionDuration,
                                                   List<Record<Event>> resultRecords) {
        // Initialize here to void multi-threading issues
        // Note: By default, one instance of processor is created across threads.
        Buffer currentBufferPerBatch = new InMemoryBuffer(keyName);
        List<Buffer> batchedBuffers = new ArrayList<>();

        LOG.info("Batch size received to lambda processor: {}", records.size());
        for (Record<Event> record : records) {
            final Event event = record.getData();

            // If the condition is false, add the event to resultRecords as-is
            if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition, event)) {
                resultRecords.add(record);
                continue;
            }

            currentBufferPerBatch.addRecord(record);
            if (ThresholdCheck.checkThresholdExceed(currentBufferPerBatch, maxEvents, maxBytes, maxCollectionDuration)) {
                batchedBuffers.add(currentBufferPerBatch);
                currentBufferPerBatch = new InMemoryBuffer(keyName);
            }
        }

        if (currentBufferPerBatch.getEventCount() > 0) {
            batchedBuffers.add(currentBufferPerBatch);
        }
        return batchedBuffers;
    }
}
