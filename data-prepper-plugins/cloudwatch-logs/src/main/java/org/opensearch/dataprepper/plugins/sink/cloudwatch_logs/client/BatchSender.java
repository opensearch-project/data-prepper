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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidParameterException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

/**
 * Sends one {@link PutLogEventsRequest} with retries. It never releases event handles, never writes the DLQ and
 * never throws for an {@link Exception} from the AWS call. It also holds the sink-level shutdown state.
 */
public class BatchSender {
    private static final Logger LOG = LoggerFactory.getLogger(BatchSender.class);

    static final Duration BACKOFF_BASE = Duration.ofMillis(500);
    static final Duration BACKOFF_CAP = Duration.ofSeconds(30);
    public static final Duration SHUTDOWN_DEADLINE = Duration.ofSeconds(30);
    static final int MULTIPLE_FAILURES_METRIC_COUNT = 5;
    static final int REQUEST_TOO_LARGE_STATUS = 413;
    private static final int MAX_BACKOFF_EXPONENT = 16;

    private final CloudWatchLogsClient cloudWatchLogsClient;
    private final CloudWatchLogsMetrics cloudWatchLogsMetrics;
    private final int maxRetries;
    private final boolean createLogGroup;
    private final boolean createLogStream;
    private final Random random;
    private final LongSupplier clock;
    private final CountDownLatch closedSignal;
    private final AtomicInteger requestsInFlight = new AtomicInteger();
    private volatile boolean closed;
    private volatile long shutdownDeadline;

    public BatchSender(final CloudWatchLogsClient cloudWatchLogsClient, final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                       final int maxRetries, final boolean createLogGroup, final boolean createLogStream,
                       final Random random, final LongSupplier clock) {
        this(cloudWatchLogsClient, cloudWatchLogsMetrics, maxRetries, createLogGroup, createLogStream, random, clock,
                new CountDownLatch(1));
    }

    BatchSender(final CloudWatchLogsClient cloudWatchLogsClient, final CloudWatchLogsMetrics cloudWatchLogsMetrics,
                final int maxRetries, final boolean createLogGroup, final boolean createLogStream,
                final Random random, final LongSupplier clock, final CountDownLatch closedSignal) {
        this.cloudWatchLogsClient = cloudWatchLogsClient;
        this.cloudWatchLogsMetrics = cloudWatchLogsMetrics;
        this.maxRetries = maxRetries;
        this.createLogGroup = createLogGroup;
        this.createLogStream = createLogStream;
        this.random = random;
        this.clock = clock;
        this.closedSignal = closedSignal;
        cloudWatchLogsMetrics.registerRequestsInFlightGauge(requestsInFlight);
    }

    /**
     * Makes at most {@code 1 + maxRetries} PutLogEvents calls, reusing the same request object for every retry. When
     * {@code retryUntilDelivered} is true, {@code maxRetries} does not apply.
     */
    public Result send(final PutLogEventsRequest request, final boolean retryUntilDelivered) {
        Exception lastException = null;
        boolean resourceCreationAttempted = false;
        int failCount = 0;
        for (int attempt = 0; retryUntilDelivered || attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                if (closed) {
                    return shutdown(request, lastException);
                }
                try {
                    if (closedSignal.await(backoffDelayMillis(attempt), TimeUnit.MILLISECONDS) || closed) {
                        return shutdown(request, lastException);
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return failed(request, lastException);
                }
            }
            if (closed && clock.getAsLong() >= shutdownDeadline) {
                return shutdown(request, lastException);
            }

            PutLogEventsResponse response = null;
            Exception failure = null;
            requestsInFlight.incrementAndGet();
            try {
                response = cloudWatchLogsClient.putLogEvents(request);
            } catch (final Exception e) {
                failure = e;
            } finally {
                requestsInFlight.decrementAndGet();
            }
            if (failure == null) {
                return delivered(request, response);
            }

            lastException = failure;
            failCount++;
            countFailedAttempt(failure, failCount);
            if (isPermanent(failure)) {
                return permanentFailure(request, lastException);
            }
            if (failure instanceof ResourceNotFoundException) {
                if (!createLogGroup && !createLogStream) {
                    cloudWatchLogsMetrics.increaseResourceNotFoundCounter(1);
                    return permanentFailure(request, lastException);
                } else if (resourceCreationAttempted) {
                    cloudWatchLogsMetrics.increaseResourceNotFoundAfterCreateCounter(1);
                } else if (closed) {
                    return shutdown(request, lastException);
                } else {
                    resourceCreationAttempted = createResources(request);
                }
            }
        }
        return failed(request, lastException);
    }

    /**
     * The caller must hold processLock. Writes {@code shutdownDeadline} before the volatile {@code closed} so any sender that
     * sees {@code closed} also sees the deadline, then wakes senders sleeping in backoff. A sender that was already closed may
     * still make a first attempt until the deadline passes.
     */
    public void close() {
        shutdownDeadline = clock.getAsLong() + SHUTDOWN_DEADLINE.toMillis();
        closed = true;
        closedSignal.countDown();
    }

    public boolean isClosed() {
        return closed;
    }

    private long backoffDelayMillis(final int retry) {
        final long cap = Math.min(BACKOFF_CAP.toMillis(), BACKOFF_BASE.toMillis() << Math.min(retry - 1, MAX_BACKOFF_EXPONENT));
        return (long) (random.nextDouble() * cap);
    }

    private void countFailedAttempt(final Exception e, final int failCount) {
        final String classification = classifyAndCountFailure(e);
        LOG.error(NOISY, "Failed to push logs (classification={}): {}", classification, e.getMessage());
        cloudWatchLogsMetrics.increaseRequestFailCounter(1);
        if (failCount % MULTIPLE_FAILURES_METRIC_COUNT == 0) {
            cloudWatchLogsMetrics.increaseRequestMultiFailCounter(1);
        }
    }

    private String classifyAndCountFailure(final Exception e) {
        if (e instanceof AwsServiceException) {
            final AwsServiceException ase = (AwsServiceException) e;
            if (ase.isThrottlingException()) {
                cloudWatchLogsMetrics.increaseThrottledCounter(1);
                return "throttled";
            }
            if (isAccessDenied(ase)) {
                cloudWatchLogsMetrics.increaseAccessDeniedCounter(1);
                return "accessDenied";
            }
        }
        return "unclassified";
    }

    private static boolean isPermanent(final Exception e) {
        return e instanceof InvalidParameterException
                || (e instanceof AwsServiceException && ((AwsServiceException) e).statusCode() == REQUEST_TOO_LARGE_STATUS);
    }

    private static boolean isAccessDenied(final AwsServiceException ase) {
        if (ase.awsErrorDetails() == null || ase.awsErrorDetails().errorCode() == null) {
            return false;
        }
        return ase.awsErrorDetails().errorCode().contains("AccessDenied");
    }

    private boolean createResources(final PutLogEventsRequest request) {
        final String logGroupName = request.logGroupName();
        final String logStreamName = request.logStreamName();
        boolean succeeded = true;
        if (createLogGroup) {
            try {
                cloudWatchLogsClient.createLogGroup(CreateLogGroupRequest.builder().logGroupName(logGroupName).build());
                LOG.info("Created log group: {}", logGroupName);
            } catch (final ResourceAlreadyExistsException e) {
                LOG.debug("Log group already exists: {}", logGroupName);
            } catch (final Exception e) {
                countResourceCreationFailure(e, "log group", logGroupName);
                succeeded = false;
            }
        }
        if (createLogStream) {
            try {
                cloudWatchLogsClient.createLogStream(CreateLogStreamRequest.builder()
                        .logGroupName(logGroupName)
                        .logStreamName(logStreamName)
                        .build());
                LOG.info("Created log stream: {}/{}", logGroupName, logStreamName);
            } catch (final ResourceAlreadyExistsException e) {
                LOG.debug("Log stream already exists: {}/{}", logGroupName, logStreamName);
            } catch (final Exception e) {
                countResourceCreationFailure(e, "log stream", logGroupName + "/" + logStreamName);
                succeeded = false;
            }
        }
        return succeeded;
    }

    private void countResourceCreationFailure(final Exception e, final String resourceType, final String resourceName) {
        if (e instanceof AwsServiceException && isAccessDenied((AwsServiceException) e)) {
            cloudWatchLogsMetrics.increaseAccessDeniedCounter(1);
            LOG.warn("Access denied creating {} '{}': {}", resourceType, resourceName, e.getMessage());
        } else {
            LOG.warn("Unable to create {} '{}': {}", resourceType, resourceName, e.getMessage());
        }
    }

    private Result delivered(final PutLogEventsRequest request, final PutLogEventsResponse response) {
        cloudWatchLogsMetrics.increaseRequestSuccessCounter(1);
        if (response != null && response.rejectedEntityInfo() != null) {
            cloudWatchLogsMetrics.increaseEntityRejectedCounter(1);
            LOG.warn(NOISY, "Entity was rejected by CloudWatch: {}", response.rejectedEntityInfo().errorTypeAsString());
        }
        final List<InputLogEvent> logEvents = request.logEvents();
        if (!logEvents.isEmpty() && logEvents.get(0).timestamp() != null) {
            cloudWatchLogsMetrics.recordStagedAgeAtDelivery(clock.getAsLong() - logEvents.get(0).timestamp());
        }
        return Result.delivered(response);
    }

    private Result failed(final PutLogEventsRequest request, final Exception lastException) {
        cloudWatchLogsMetrics.increaseLogEventFailCounter(request.logEvents().size());
        return Result.failed(lastException);
    }

    private Result permanentFailure(final PutLogEventsRequest request, final Exception lastException) {
        cloudWatchLogsMetrics.increaseLogEventFailCounter(request.logEvents().size());
        return Result.permanentFailure(lastException);
    }

    private Result shutdown(final PutLogEventsRequest request, final Exception lastException) {
        cloudWatchLogsMetrics.increaseLogEventFailCounter(request.logEvents().size());
        return Result.shutdown(lastException);
    }

    @Getter
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Result {
        public enum Status { DELIVERED, FAILED, PERMANENT_FAILURE, SHUTDOWN }

        private final Status status;
        /** The 2xx response; only set for {@link Status#DELIVERED}. */
        private final PutLogEventsResponse response;
        /** The last attempt's exception for {@link Status#FAILED} and {@link Status#SHUTDOWN}; may be null for SHUTDOWN. */
        private final Exception lastException;

        static Result delivered(final PutLogEventsResponse response) {
            return new Result(Status.DELIVERED, response, null);
        }

        static Result failed(final Exception lastException) {
            return new Result(Status.FAILED, null, lastException);
        }

        static Result permanentFailure(final Exception lastException) {
            return new Result(Status.PERMANENT_FAILURE, null, lastException);
        }

        static Result shutdown(final Exception lastException) {
            return new Result(Status.SHUTDOWN, null, lastException);
        }
    }
}
