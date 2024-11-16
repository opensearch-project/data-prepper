package org.opensearch.dataprepper.plugins.lambda.common.util;

import com.linecorp.armeria.client.retry.Backoff;
import org.opensearch.dataprepper.plugins.lambda.common.exception.S3RetriesExhaustedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.RequestTooLargeException;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Static util to help with lambda invocations and retry logic
 */
public class InvocationUtil {
    static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();
    static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();
    static final double JITTER_RATE = 0.20;
    private static final int MAX_ATTEMPT_COUNT = 3;
    private static final Logger LOG = LoggerFactory.getLogger(InvocationUtil.class);

    public static void invokeLambdaWithRetry(final LambdaAsyncClient lambdaAsyncClient,
                                             InvokeRequest request,
                                             List<CompletableFuture<InvokeResponse>> futureList,
                                             Consumer<InvokeResponse> successHandler,
                                             Consumer<Throwable> failureHandler) {
        final Backoff backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
        AtomicInteger failedAttemptCount = new AtomicInteger();
        while (failedAttemptCount.get() < MAX_ATTEMPT_COUNT) {
            CompletableFuture<InvokeResponse> future = lambdaAsyncClient.invoke(request);
            futureList.add(future);
            future.thenAccept(successHandler).exceptionally(throwable -> {
                if (throwable instanceof TooManyRequestsException) {
                    failedAttemptCount.getAndIncrement();
                    applyBackoff(backoff, failedAttemptCount);
                } else if (throwable instanceof RequestTooLargeException) {
                    //We should possibly split this out into more chunks
                    //Ideally, we shouldn't get into here
                    LOG.error("Request too large exception, please check your payload size", throwable);
                } else {
                    failureHandler.accept(throwable);
                }
                return null;
            });
        }
    }

    private static void applyBackoff(Backoff backoff, AtomicInteger failedAttemptCount) {
        final long delayMillis = backoff.nextDelayMillis(failedAttemptCount.get());
        if (delayMillis < 0) {
            Thread.currentThread().interrupt();
            throw new S3RetriesExhaustedException("Lambda retries exhausted. Current Lambda concurrency setting is either too small or too busy.");
        }
        final Duration delayDuration = Duration.ofMillis(delayMillis);
        LOG.info("Pausing Lambda processing for {}.{} seconds due to TooManyRequetsException.",
                delayDuration.getSeconds(), delayDuration.toMillisPart());
        try {
            Thread.sleep(delayMillis);
        } catch (final InterruptedException e) {
            LOG.error("Thread is interrupted while waiting for backoff retry.", e);
        }
    }
}
