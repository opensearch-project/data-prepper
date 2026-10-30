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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidParameterException;
import software.amazon.awssdk.services.cloudwatchlogs.model.OperationAbortedException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedEntityInfo;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BatchSenderTest {
    private static final String LOG_GROUP = "testGroup";
    private static final String LOG_STREAM = "testStream";
    private static final long NOW = 1_000_000L;
    private static final long EVENT_TIMESTAMP = NOW - 1234L;
    private static final double MAX_DRAW = 0.999_999;

    private CloudWatchLogsClient client;
    private CloudWatchLogsMetrics metrics;
    private Random random;
    private CountDownLatch closedSignal;
    private AtomicLong clock;
    private PutLogEventsRequest request;

    @BeforeEach
    void setUp() throws InterruptedException {
        client = mock(CloudWatchLogsClient.class);
        metrics = mock(CloudWatchLogsMetrics.class);
        random = mock(Random.class);
        when(random.nextDouble()).thenReturn(MAX_DRAW);
        closedSignal = mock(CountDownLatch.class);
        when(closedSignal.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
        clock = new AtomicLong(NOW);
        request = PutLogEventsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamName(LOG_STREAM)
                .logEvents(List.of(
                        InputLogEvent.builder().message("a").timestamp(EVENT_TIMESTAMP).build(),
                        InputLogEvent.builder().message("b").timestamp(EVENT_TIMESTAMP).build()))
                .build();
    }

    private BatchSender createSender(final int maxRetries, final boolean createLogGroup, final boolean createLogStream) {
        return new BatchSender(client, metrics, maxRetries, createLogGroup, createLogStream, random, clock::get, closedSignal);
    }

    private static CloudWatchLogsException cloudWatchLogsException(final String errorCode) {
        return (CloudWatchLogsException) CloudWatchLogsException.builder()
                .message("failure " + errorCode)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
                .build();
    }

    private static ResourceNotFoundException resourceNotFound() {
        return ResourceNotFoundException.builder().message("not found").build();
    }

    private static long fullJitterCap(final int retry) {
        return Math.min(30_000L, 500L * (1L << (retry - 1)));
    }

    /** Stand-in for STS's StsException, which is not on this plugin's classpath; like it, a plain AwsServiceException subclass. */
    private static final class StsException extends AwsServiceException {
        StsException() {
            super(AwsServiceException.builder().message("sts assume role failed"));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 15})
    void send_alwaysFails_makesMaxRetriesPlusOneCallsWithSameRequestThenFailed(final int maxRetries) {
        final CloudWatchLogsException failure = cloudWatchLogsException("InternalFailure");
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(failure);

        final BatchSender.Result result = createSender(maxRetries, false, false).send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.FAILED));
        assertThat(result.getLastException(), sameInstance(failure));
        final ArgumentCaptor<PutLogEventsRequest> sent = ArgumentCaptor.forClass(PutLogEventsRequest.class);
        verify(client, times(maxRetries + 1)).putLogEvents(sent.capture());
        for (final PutLogEventsRequest sentRequest : sent.getAllValues()) {
            assertThat(sentRequest, sameInstance(request));
        }
        verify(metrics, times(maxRetries + 1)).increaseRequestFailCounter(1);
        verify(metrics, times((maxRetries + 1) / 5)).increaseRequestMultiFailCounter(1);
        verify(metrics).increaseLogEventFailCounter(2);
        verify(metrics, never()).increaseRequestSuccessCounter(anyInt());
    }

    @ParameterizedTest
    @ValueSource(doubles = {0.0, MAX_DRAW})
    void send_retries_eachSleepIsDrawTimesFullJitterCap(final double draw) throws InterruptedException {
        // Full jitter: the sleep before retry n is drawn from [0, min(30 s, 500 ms * 2^(n-1))].
        final int maxRetries = 15;
        when(random.nextDouble()).thenReturn(draw);
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(cloudWatchLogsException("InternalFailure"));

        createSender(maxRetries, false, false).send(request, false);

        final ArgumentCaptor<Long> delays = ArgumentCaptor.forClass(Long.class);
        verify(closedSignal, times(maxRetries)).await(delays.capture(), eq(TimeUnit.MILLISECONDS));
        final List<Long> values = delays.getAllValues();
        for (int retry = 1; retry <= maxRetries; retry++) {
            assertThat(values.get(retry - 1), equalTo((long) (draw * fullJitterCap(retry))));
        }
        assertThat(values.get(maxRetries - 1), lessThan(30_001L));
    }

    @Test
    void send_retryUntilDelivered_ignoresMaxRetriesUntilDelivered() throws InterruptedException {
        final int failures = 40;
        final PutLogEventsResponse response = PutLogEventsResponse.builder().build();
        final AtomicInteger calls = new AtomicInteger();
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            if (calls.incrementAndGet() <= failures) {
                throw cloudWatchLogsException("ServiceUnavailable");
            }
            return response;
        });

        final BatchSender.Result result = createSender(1, false, false).send(request, true);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.DELIVERED));
        verify(client, times(failures + 1)).putLogEvents(any(PutLogEventsRequest.class));
        final ArgumentCaptor<Long> delays = ArgumentCaptor.forClass(Long.class);
        verify(closedSignal, times(failures)).await(delays.capture(), eq(TimeUnit.MILLISECONDS));
        assertThat(delays.getValue(), equalTo((long) (MAX_DRAW * 30_000L)));
        verify(metrics, never()).increaseLogEventFailCounter(anyInt());
    }

    @Test
    void send_retryUntilDeliveredAndClosed_shutdown() throws InterruptedException {
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(cloudWatchLogsException("ServiceUnavailable"));
        when(closedSignal.await(anyLong(), any(TimeUnit.class))).thenReturn(false, false, true);

        final BatchSender.Result result = createSender(1, false, false).send(request, true);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.SHUTDOWN));
        verify(client, times(3)).putLogEvents(any(PutLogEventsRequest.class));
        verify(metrics).increaseLogEventFailCounter(2);
    }

    static Stream<Exception> permanentExceptions() {
        return Stream.of(
                InvalidParameterException.builder().message("invalid").build(),
                AwsServiceException.builder().message("too large").statusCode(BatchSender.REQUEST_TOO_LARGE_STATUS).build());
    }

    @ParameterizedTest
    @MethodSource("permanentExceptions")
    void send_permanentFailure_failedAfterOneAttemptInBothModes(final Exception exception) throws InterruptedException {
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(exception);

        for (final boolean retryUntilDelivered : new boolean[] {false, true}) {
            final BatchSender.Result result = createSender(5, false, false).send(request, retryUntilDelivered);

            assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.PERMANENT_FAILURE));
            assertThat(result.getLastException(), sameInstance(exception));
        }
        verify(client, times(2)).putLogEvents(any(PutLogEventsRequest.class));
        verify(closedSignal, never()).await(anyLong(), any(TimeUnit.class));
    }

    @Test
    void backoffConstants_haveSpecifiedValues() {
        assertThat(BatchSender.BACKOFF_BASE, equalTo(Duration.ofMillis(500)));
        assertThat(BatchSender.BACKOFF_CAP, equalTo(Duration.ofSeconds(30)));
        assertThat(BatchSender.MULTIPLE_FAILURES_METRIC_COUNT, equalTo(5));
    }

    static Stream<Exception> retryableExceptions() {
        return Stream.of(
                CloudWatchLogsException.builder().message("cwl").build(),
                new StsException(),
                AwsServiceException.builder().message("aws").build(),
                SdkClientException.create("sdk client"),
                AbortedException.create("aborted"),
                ApiCallAttemptTimeoutException.create(10_000L),
                new RuntimeException("runtime"));
    }

    @ParameterizedTest
    @MethodSource("retryableExceptions")
    void send_anyExceptionType_isRetried(final Exception exception) {
        final PutLogEventsResponse response = PutLogEventsResponse.builder().build();
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(exception).thenReturn(response);

        final BatchSender.Result result = createSender(5, false, false).send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.DELIVERED));
        assertThat(result.getResponse(), sameInstance(response));
        verify(client, times(2)).putLogEvents(any(PutLogEventsRequest.class));
        verify(metrics).increaseRequestFailCounter(1);
        verify(metrics).increaseRequestSuccessCounter(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void send_resourceNotFoundWithCreateFlag_createsOnceThenBacksOffThenDelivered(final boolean createLogStream)
            throws InterruptedException {
        // The ResourceNotFound call that triggers creation counts as a failed attempt, and the retry after creation still backs off.
        when(client.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(resourceNotFound())
                .thenReturn(PutLogEventsResponse.builder().build());

        final BatchSender.Result result = createSender(5, true, createLogStream).send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.DELIVERED));
        final ArgumentCaptor<CreateLogGroupRequest> groupRequest = ArgumentCaptor.forClass(CreateLogGroupRequest.class);
        verify(client).createLogGroup(groupRequest.capture());
        assertThat(groupRequest.getValue().logGroupName(), equalTo(LOG_GROUP));
        if (createLogStream) {
            final ArgumentCaptor<CreateLogStreamRequest> streamRequest = ArgumentCaptor.forClass(CreateLogStreamRequest.class);
            verify(client).createLogStream(streamRequest.capture());
            assertThat(streamRequest.getValue().logGroupName(), equalTo(LOG_GROUP));
            assertThat(streamRequest.getValue().logStreamName(), equalTo(LOG_STREAM));
        } else {
            verify(client, never()).createLogStream(any(CreateLogStreamRequest.class));
        }
        verify(closedSignal, times(1)).await(anyLong(), any(TimeUnit.class));
        verify(closedSignal, times(1)).await((long) (MAX_DRAW * fullJitterCap(1)), TimeUnit.MILLISECONDS);
        verify(client, times(2)).putLogEvents(any(PutLogEventsRequest.class));
        verify(metrics).increaseRequestFailCounter(1);
        verify(metrics, never()).increaseResourceNotFoundCounter(anyInt());
        verify(metrics, never()).increaseResourceNotFoundAfterCreateCounter(anyInt());
    }

    @Test
    void send_rnfAgainAfterCreate_noSecondCreateCountedAsAttempt() {
        // The ResourceNotFound that triggered creation counts as an attempt, so max_retries = 1 allows exactly two calls.
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(resourceNotFound());

        final BatchSender.Result result = createSender(1, true, false).send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.FAILED));
        verify(client, times(2)).putLogEvents(any(PutLogEventsRequest.class));
        verify(client, times(1)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(client, never()).createLogStream(any(CreateLogStreamRequest.class));
        verify(metrics, times(2)).increaseRequestFailCounter(1);
        verify(metrics).increaseResourceNotFoundAfterCreateCounter(1);
        verify(metrics, never()).increaseResourceNotFoundCounter(anyInt());
    }

    @Test
    void send_resourceNotFoundWithoutCreateFlags_permanentFailureAfterOneAttemptInBothModes() throws InterruptedException {
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(resourceNotFound());

        for (final boolean retryUntilDelivered : new boolean[] {false, true}) {
            final BatchSender.Result result = createSender(3, false, false).send(request, retryUntilDelivered);

            assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.PERMANENT_FAILURE));
        }
        verify(client, times(2)).putLogEvents(any(PutLogEventsRequest.class));
        verify(metrics, times(2)).increaseResourceNotFoundCounter(1);
        verify(closedSignal, never()).await(anyLong(), any(TimeUnit.class));
        verify(client, never()).createLogGroup(any(CreateLogGroupRequest.class));
        verify(client, never()).createLogStream(any(CreateLogStreamRequest.class));
    }

    @Test
    void send_resourceAlreadyExists_treatedAsSuccess() {
        // An existing log group must not stop creation of the log stream.
        when(client.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(resourceNotFound())
                .thenReturn(PutLogEventsResponse.builder().build());
        when(client.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenThrow(ResourceAlreadyExistsException.builder().message("exists").build());

        final BatchSender.Result result = createSender(1, true, true).send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.DELIVERED));
        verify(client).createLogStream(any(CreateLogStreamRequest.class));
        verify(metrics, never()).increaseAccessDeniedCounter(anyInt());
    }

    static Stream<Exception> creationFailures() {
        return Stream.of(new StsException(), OperationAbortedException.builder().message("concurrent create").build());
    }

    @ParameterizedTest
    @MethodSource("creationFailures")
    void send_creationThrows_loggedLoopContinues(final Exception creationFailure) {
        // A failed creation call does not consume a PutLogEvents attempt.
        when(client.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(resourceNotFound())
                .thenReturn(PutLogEventsResponse.builder().build());
        when(client.createLogGroup(any(CreateLogGroupRequest.class))).thenThrow(creationFailure);

        final BatchSender.Result result = createSender(1, true, true).send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.DELIVERED));
        verify(client, times(2)).putLogEvents(any(PutLogEventsRequest.class));
        verify(client).createLogStream(any(CreateLogStreamRequest.class));
        verify(metrics, times(1)).increaseRequestFailCounter(1);
    }

    @Test
    void send_creationFailed_retriedOnNextResourceNotFound() {
        when(client.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(resourceNotFound())
                .thenThrow(resourceNotFound())
                .thenReturn(PutLogEventsResponse.builder().build());
        when(client.createLogGroup(any(CreateLogGroupRequest.class)))
                .thenThrow(cloudWatchLogsException("ThrottlingException"))
                .thenReturn(null);

        final BatchSender.Result result = createSender(5, true, false).send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.DELIVERED));
        verify(client, times(2)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(client, times(3)).putLogEvents(any(PutLogEventsRequest.class));
        verify(metrics, never()).increaseResourceNotFoundAfterCreateCounter(anyInt());
    }

    @Test
    void send_creationAccessDenied_countedAsAccessDenied() {
        when(client.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(resourceNotFound())
                .thenReturn(PutLogEventsResponse.builder().build());
        when(client.createLogGroup(any(CreateLogGroupRequest.class))).thenThrow(cloudWatchLogsException("AccessDeniedException"));

        createSender(1, true, false).send(request, false);

        verify(metrics).increaseAccessDeniedCounter(1);
    }

    @Test
    void send_resourceNotFoundWhenClosed_noCreation() {
        final AtomicReference<BatchSender> sender = new AtomicReference<>();
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            sender.get().close();
            throw resourceNotFound();
        });
        sender.set(createSender(5, true, true));

        final BatchSender.Result result = sender.get().send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.SHUTDOWN));
        verify(client, never()).createLogGroup(any(CreateLogGroupRequest.class));
        verify(client, never()).createLogStream(any(CreateLogStreamRequest.class));
        verify(client, times(1)).putLogEvents(any(PutLogEventsRequest.class));
    }

    @Test
    void send_backoffInterrupted_failedWithInterruptFlagSet() throws InterruptedException {
        final CloudWatchLogsException failure = cloudWatchLogsException("InternalFailure");
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(failure);
        when(closedSignal.await(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());

        final BatchSender.Result result = createSender(5, false, false).send(request, false);
        final boolean interrupted = Thread.interrupted();

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.FAILED));
        assertThat(result.getLastException(), sameInstance(failure));
        assertThat(interrupted, equalTo(true));
        verify(client, times(1)).putLogEvents(any(PutLogEventsRequest.class));
        verify(metrics).increaseLogEventFailCounter(2);
    }

    @Test
    void send_closedAfterFirstFailure_shutdownAfterOneCall() throws InterruptedException {
        final AtomicReference<BatchSender> sender = new AtomicReference<>();
        final CloudWatchLogsException failure = cloudWatchLogsException("InternalFailure");
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            sender.get().close();
            throw failure;
        });
        sender.set(createSender(5, false, false));

        final BatchSender.Result result = sender.get().send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.SHUTDOWN));
        assertThat(result.getLastException(), sameInstance(failure));
        verify(client, times(1)).putLogEvents(any(PutLogEventsRequest.class));
        verify(closedSignal, never()).await(anyLong(), any(TimeUnit.class));
        verify(metrics).increaseLogEventFailCounter(2);
    }

    @Test
    void send_sleepingWhenCloseCalled_wakesEarlyShutdown() throws Exception {
        final int callsBeforeLongSleep = 7;
        when(random.nextDouble()).thenReturn(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, MAX_DRAW);
        final AtomicInteger calls = new AtomicInteger();
        final CountDownLatch reachedLongSleep = new CountDownLatch(1);
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            if (calls.incrementAndGet() == callsBeforeLongSleep) {
                reachedLongSleep.countDown();
            }
            throw cloudWatchLogsException("InternalFailure");
        });
        final BatchSender sender = new BatchSender(client, metrics, 15, false, false, random, clock::get, new CountDownLatch(1));
        final AtomicReference<BatchSender.Result> result = new AtomicReference<>();
        final Thread senderThread = new Thread(() -> result.set(sender.send(request, false)));
        senderThread.start();
        assertThat(reachedLongSleep.await(10, TimeUnit.SECONDS), equalTo(true));
        while (senderThread.getState() != Thread.State.TIMED_WAITING && senderThread.isAlive()) {
            Thread.onSpinWait();
        }

        final long start = System.nanoTime();
        sender.close();
        senderThread.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(senderThread.isAlive(), equalTo(false));
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), lessThan(10_000L));
        assertThat(result.get().getStatus(), equalTo(BatchSender.Result.Status.SHUTDOWN));
        assertThat(calls.get(), equalTo(callsBeforeLongSleep));
    }

    @Test
    void send_closedWhileSleepTimesOut_shutdownNoFurtherCall() throws InterruptedException {
        final AtomicReference<BatchSender> sender = new AtomicReference<>();
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(cloudWatchLogsException("InternalFailure"));
        when(closedSignal.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            sender.get().close();
            return false;
        });
        sender.set(createSender(5, false, false));

        final BatchSender.Result result = sender.get().send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.SHUTDOWN));
        verify(client, times(1)).putLogEvents(any(PutLogEventsRequest.class));
    }

    @ParameterizedTest
    @CsvSource({"29999, 1", "30000, 0"})
    void send_closedBeforeFirstAttempt_attemptsOnlyBeforeThirtySecondShutdownDeadline(final long elapsedMillis, final int expectedCalls) {
        // After close(), a send still makes its first attempt until the 30 s shutdown deadline, and none at or after it.
        final CloudWatchLogsException failure = cloudWatchLogsException("InternalFailure");
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(failure);
        final BatchSender sender = createSender(5, false, false);
        sender.close();
        clock.addAndGet(elapsedMillis);

        final BatchSender.Result result = sender.send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.SHUTDOWN));
        if (expectedCalls == 0) {
            assertThat(result.getLastException(), nullValue());
        } else {
            assertThat(result.getLastException(), sameInstance(failure));
        }
        verify(client, times(expectedCalls)).putLogEvents(any(PutLogEventsRequest.class));
        verify(metrics).increaseLogEventFailCounter(2);
    }

    @Test
    void send_delivered_countsSuccessAndStagedAge() {
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(PutLogEventsResponse.builder().build());

        final BatchSender.Result result = createSender(5, false, false).send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.DELIVERED));
        verify(metrics).increaseRequestSuccessCounter(1);
        verify(metrics).recordStagedAgeAtDelivery(NOW - EVENT_TIMESTAMP);
        verify(metrics, never()).increaseLogEventFailCounter(anyInt());
        verify(metrics, never()).increaseEntityRejectedCounter(anyInt());
    }

    @Test
    void send_deliveredWithRejectedEntity_countsEntityRejected() {
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenReturn(PutLogEventsResponse.builder()
                .rejectedEntityInfo(RejectedEntityInfo.builder().errorType("InvalidEntity").build())
                .build());

        final BatchSender.Result result = createSender(5, false, false).send(request, false);

        assertThat(result.getStatus(), equalTo(BatchSender.Result.Status.DELIVERED));
        verify(metrics).increaseEntityRejectedCounter(1);
    }

    @Test
    void send_throttled_countsThrottledPerAttempt() {
        final CloudWatchLogsException throttled = (CloudWatchLogsException) CloudWatchLogsException.builder()
                .message("slow down")
                .statusCode(429)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("ThrottlingException").build())
                .build();
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(throttled);

        createSender(2, false, false).send(request, false);

        verify(metrics, times(3)).increaseThrottledCounter(1);
        verify(metrics, never()).increaseAccessDeniedCounter(anyInt());
    }

    @Test
    void send_accessDenied_countsAccessDeniedPerAttempt() {
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(cloudWatchLogsException("AccessDeniedException"));

        createSender(2, false, false).send(request, false);

        verify(metrics, times(3)).increaseAccessDeniedCounter(1);
        verify(metrics, never()).increaseThrottledCounter(anyInt());
    }

    @Test
    void send_duringCall_requestsInFlightIsOneThenZero() {
        final ArgumentCaptor<AtomicInteger> gauge = ArgumentCaptor.forClass(AtomicInteger.class);
        final BatchSender sender = createSender(5, false, false);
        verify(metrics).registerRequestsInFlightGauge(gauge.capture());
        final AtomicInteger inFlightDuringCall = new AtomicInteger(-1);
        when(client.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            inFlightDuringCall.set(gauge.getValue().get());
            throw cloudWatchLogsException("InternalFailure");
        }).thenAnswer(invocation -> PutLogEventsResponse.builder().build());

        sender.send(request, false);

        assertThat(inFlightDuringCall.get(), equalTo(1));
        assertThat(gauge.getValue().get(), equalTo(0));
    }
}
