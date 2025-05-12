package org.opensearch.dataprepper.plugins.kinesis.source.apihandler;

import com.linecorp.armeria.client.retry.Backoff;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisRetriesExhaustedException;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

@Slf4j
public class KinesisClientApiRetryHandler {
    private final Backoff backoff;
    private final int maxRetryCount;

    public KinesisClientApiRetryHandler(final Backoff backoff, final int maxRetryCount) {
        if (maxRetryCount <= 0) {
            throw new IllegalArgumentException("Maximum Retry count should be strictly greater than zero.");
        }
        this.backoff = Objects.requireNonNull(backoff, "Backoff cannot be null");
        this.maxRetryCount = maxRetryCount;
    }

    public <T> T executeWithRetry(String operationName, Supplier<T> operation,
                                  ExceptionHandler exceptionHandler) {

        Objects.requireNonNull(operationName, "Operation name cannot be null");
        Objects.requireNonNull(operation, "Operation cannot be null");
        Objects.requireNonNull(exceptionHandler, "Exception handler cannot be null");

        for (int attempt = 0; attempt < maxRetryCount; attempt++) {
            try {
                return operation.get();
            } catch (Exception ex) {
                exceptionHandler.handle(ex, attempt);
            }
            applyBackoff(attempt);
        }
        throw new KinesisRetriesExhaustedException(
                String.format("Failed to execute %s after %d retries", operationName, maxRetryCount));
    }

    private void applyBackoff(int attempt) {
        final long delayMillis = backoff.nextDelayMillis(attempt);
        if (delayMillis < 0) {
            throw new KinesisRetriesExhaustedException(
                    "Retries exhausted. Make sure that configuration is valid and required permissions are present.");
        }
        sleep(delayMillis);
    }

    private void sleep(long delayMillis) {
        if (delayMillis <= 0) {
            return;
        }
        final Duration delayDuration = Duration.ofMillis(delayMillis);
        log.info("Pausing request for {}.{} seconds due to an error in processing.",
                delayDuration.getSeconds(), delayDuration.toMillisPart());
        try {
            Thread.sleep(delayMillis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Thread interrupted while waiting for retry", e);
            throw new KinesisRetriesExhaustedException("Thread interrupted while waiting for retry");
        }
    }

    @FunctionalInterface
    public interface ExceptionHandler {
        void handle(Exception ex, int attempt);
    }
}