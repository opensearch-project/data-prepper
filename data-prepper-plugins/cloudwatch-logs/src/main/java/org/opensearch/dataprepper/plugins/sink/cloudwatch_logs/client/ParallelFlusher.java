/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * Sends batches on the sink-owned flusher pool and waits for them. Each submitted batch is owned end to end by its pool task, so
 * this class never cancels a future, never interrupts a pool thread and never acquires processLock.
 */
@RequiredArgsConstructor
public class ParallelFlusher {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelFlusher.class);

    private final ExecutorService pool;
    private final BatchSender batchSender;
    private final OutcomeHandler outcomeHandler;
    private final String logGroup;
    private final String logStream;

    /**
     * Submits one task per batch. A rejected batch has no task to own it, so it is settled on this thread with reason shutdown.
     * Waits for every submitted task without being interruptible, then rethrows the first task Error other than
     * StackOverflowError.
     */
    public void flush(final List<Batch> batches) {
        final List<Future<?>> futures = new ArrayList<>(batches.size());
        try {
            for (final Batch batch : batches) {
                try {
                    futures.add(pool.submit(() -> runTask(batch)));
                } catch (final RejectedExecutionException e) {
                    LOG.warn("Flusher pool is shut down; settling a batch of {} events with reason shutdown",
                            batch.getEventHandles().size());
                    outcomeHandler.settle(batch, OutcomeHandler.Reason.SHUTDOWN, null);
                    continue;
                }
                batch.markSettlementStarted();
            }
        } finally {
            awaitAll(futures);
        }
    }

    private static void awaitAll(final List<Future<?>> futures) {
        boolean interrupted = false;
        Error error = null;
        for (final Future<?> future : futures) {
            while (true) {
                try {
                    future.get();
                    break;
                } catch (final InterruptedException e) {
                    interrupted = true;
                } catch (final ExecutionException e) {
                    LOG.error("A flusher task failed after settling its batch", e.getCause());
                    if (error == null && e.getCause() instanceof Error && !(e.getCause() instanceof StackOverflowError)) {
                        error = (Error) e.getCause();
                    }
                    break;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (error != null) {
            throw error;
        }
    }

    PutLogEventsRequest buildRequest(final Batch batch) {
        final long timestamp = System.currentTimeMillis();
        final List<InputLogEvent> logEvents = new ArrayList<>(batch.getMessages().size());
        for (final byte[] message : batch.getMessages()) {
            logEvents.add(InputLogEvent.builder().message(new String(message, StandardCharsets.UTF_8)).timestamp(timestamp).build());
        }
        final PutLogEventsRequest.Builder requestBuilder = PutLogEventsRequest.builder()
                .logEvents(logEvents)
                .logGroupName(logGroup)
                .logStreamName(logStream);
        if (batch.getEntity() != null) {
            requestBuilder.entity(batch.getEntity());
        }
        final Map<String, String> headers = batch.getHeaders();
        if (headers != null && !headers.isEmpty()) {
            final AwsRequestOverrideConfiguration.Builder overrideBuilder = AwsRequestOverrideConfiguration.builder();
            headers.forEach(overrideBuilder::putHeader);
            requestBuilder.overrideConfiguration(overrideBuilder.build());
        }
        return requestBuilder.build();
    }

    /**
     * Its finally block releases with false every handle outcome handling did not release. An interrupt seen by the sender is
     * cleared during outcome handling, so the DLQ write is not aborted, and restored afterwards.
     */
    void runTask(final Batch batch) {
        boolean interrupted = false;
        try {
            final BatchSender.Result result;
            try {
                result = batchSender.send(buildRequest(batch), outcomeHandler.mustRetryUntilDelivered(batch));
            } catch (final Exception | StackOverflowError e) {
                interrupted = Thread.interrupted();
                LOG.error("Unexpected error sending a batch of {} events", batch.getEventHandles().size(), e);
                outcomeHandler.terminalStep(batch, OutcomeHandler.Reason.UNEXPECTED_ERROR, e);
                return;
            }
            interrupted = Thread.interrupted();
            outcomeHandler.handleResult(batch, result);
        } finally {
            outcomeHandler.releaseUnreleased(batch);
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
