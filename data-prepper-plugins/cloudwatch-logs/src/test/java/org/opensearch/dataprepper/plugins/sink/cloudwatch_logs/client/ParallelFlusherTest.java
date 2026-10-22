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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.invocation.Invocation;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.dlq.CloudWatchLogsSinkDlqData;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_DLQ_WRITE_FAILED;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_RETRIES_EXHAUSTED;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_SHUTDOWN;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_UNEXPECTED_ERROR;

class ParallelFlusherTest {
    private static final String LOG_GROUP = "testGroup";
    private static final String LOG_STREAM = "testStream";
    private static final long WAIT_SECONDS = 10;
    private static final long NEGATIVE_CHECK_MILLIS = 300;

    enum DlqMode { NONE, OK, FALSE, THROWS }

    enum Failure { SEND_RUNTIME_EXCEPTION, SEND_STACK_OVERFLOW_ERROR, BUILD_RUNTIME_EXCEPTION }

    private CloudWatchLogsMetrics metrics;
    private BatchSender batchSender;
    private DlqPushHandler dlqPushHandler;
    private final List<List<DlqObject>> performed = Collections.synchronizedList(new ArrayList<>());
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        metrics = mock(CloudWatchLogsMetrics.class);
        batchSender = mock(BatchSender.class);
        dlqPushHandler = mock(DlqPushHandler.class);
        final PluginSetting pluginSetting = new PluginSetting("dlq", Map.of());
        pluginSetting.setPipelineName("pipeline");
        when(dlqPushHandler.getPluginSetting()).thenReturn(pluginSetting);
    }

    @AfterEach
    void tearDown() {
        Thread.interrupted();
        if (pool != null) {
            pool.shutdown();
        }
    }

    private OutcomeHandler outcomeHandlerFor(final DlqMode dlqMode) {
        if (dlqMode == DlqMode.NONE) {
            return new OutcomeHandler(null, metrics);
        }
        doAnswer(invocation -> {
            performed.add(new ArrayList<>(invocation.getArgument(0)));
            if (dlqMode == DlqMode.THROWS) {
                throw new RuntimeException("dlq writer failed");
            }
            return dlqMode == DlqMode.OK;
        }).when(dlqPushHandler).perform(any());
        return new OutcomeHandler(dlqPushHandler, metrics);
    }

    /** Daemon threads, so a failing latch-driven test cannot hang the test JVM. */
    private static ExecutorService daemonPool(final int threads) {
        final AtomicInteger count = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, runnable -> {
            final Thread thread = new Thread(runnable, "flusher-pool-" + count.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    private static Thread daemonThread(final Runnable runnable, final String name) {
        final Thread thread = new Thread(runnable, name);
        thread.setDaemon(true);
        return thread;
    }

    private ParallelFlusher flusher(final ExecutorService executorService, final OutcomeHandler outcomeHandler) {
        return new ParallelFlusher(executorService, batchSender, outcomeHandler, LOG_GROUP, LOG_STREAM);
    }

    private static List<EventHandle> handles(final int count) {
        return IntStream.range(0, count).mapToObj(i -> mock(EventHandle.class)).collect(Collectors.toList());
    }

    private static Batch batchOf(final String prefix, final List<EventHandle> handles) {
        final List<byte[]> messages = IntStream.range(0, handles.size())
                .mapToObj(i -> (prefix + "-" + i).getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
        return new Batch(handles, messages, null, null);
    }

    private static BatchSender.Result delivered() {
        return BatchSender.Result.delivered(PutLogEventsResponse.builder().build());
    }

    /** Disposition counts keyed by "counter:reason", read from every call made on the metrics mock. */
    private Map<String, Integer> dispositions() {
        final Map<String, Integer> counts = new HashMap<>();
        for (final Invocation invocation : mockingDetails(metrics).getInvocations()) {
            final String name = invocation.getMethod().getName();
            if (name.equals("increaseLogEventSuccessCounter")) {
                counts.merge("succeeded", invocation.<Integer>getArgument(0), Integer::sum);
            } else if (name.equals("increaseReleasedWithoutDeliveryCounter")) {
                counts.merge("releasedWithoutDelivery:" + invocation.getArgument(0), invocation.<Integer>getArgument(1), Integer::sum);
            } else if (name.equals("increaseEventsSentToDlqCounter")) {
                counts.merge("sentToDlq:" + invocation.getArgument(0), invocation.<Integer>getArgument(1), Integer::sum);
            } else if (name.equals("increaseEventsNegativelyAckedCounter")) {
                counts.merge("negativelyAcked:" + invocation.getArgument(0), invocation.<Integer>getArgument(1), Integer::sum);
            }
        }
        return counts;
    }

    private int dispositionTotal() {
        return dispositions().values().stream().mapToInt(Integer::intValue).sum();
    }

    @Test
    void buildRequest_allEventsShareOneTimestampInOrder() {
        final Entity entity = Entity.builder().keyAttributes(Map.of("Type", "Service", "Name", "svc")).build();
        final Map<String, String> headers = Map.of("x-source-name", "some-name", "x-source-type", "some-type");
        final List<byte[]> messages = IntStream.range(0, 50)
                .mapToObj(i -> ("m" + i).getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
        final Batch batch = new Batch(handles(50), messages, entity, headers);

        final PutLogEventsRequest request = flusher(mock(ExecutorService.class), outcomeHandlerFor(DlqMode.NONE)).buildRequest(batch);

        assertThat(request.logGroupName(), equalTo(LOG_GROUP));
        assertThat(request.logStreamName(), equalTo(LOG_STREAM));
        assertThat(request.logEvents().stream().map(InputLogEvent::message).collect(Collectors.toList()),
                equalTo(IntStream.range(0, 50).mapToObj(i -> "m" + i).collect(Collectors.toList())));
        assertThat(request.logEvents().stream().map(InputLogEvent::timestamp).distinct().count(), equalTo(1L));
        assertThat(request.entity(), equalTo(entity));
        final Map<String, List<String>> requestHeaders = request.overrideConfiguration().get().headers();
        assertThat(requestHeaders.get("x-source-name"), equalTo(List.of("some-name")));
        assertThat(requestHeaders.get("x-source-type"), equalTo(List.of("some-type")));
    }

    @Test
    void buildRequest_noEntityNoHeaders_requestHasNeither() {
        final ParallelFlusher parallelFlusher = flusher(mock(ExecutorService.class), outcomeHandlerFor(DlqMode.NONE));

        final PutLogEventsRequest withNull = parallelFlusher.buildRequest(batchOf("a", handles(1)));
        final PutLogEventsRequest withEmpty = parallelFlusher.buildRequest(
                new Batch(handles(1), List.of("a".getBytes(StandardCharsets.UTF_8)), null, Map.of()));

        assertThat(withNull.entity(), nullValue());
        assertThat(withNull.overrideConfiguration().isPresent(), equalTo(false));
        assertThat(withEmpty.overrideConfiguration().isPresent(), equalTo(false));
    }

    private static Stream<Arguments> unexpectedErrorMatrix() {
        return Stream.of(Failure.values())
                .flatMap(failure -> Stream.of(DlqMode.values()).map(dlqMode -> arguments(failure, dlqMode)));
    }

    @ParameterizedTest
    @MethodSource("unexpectedErrorMatrix")
    void runTask_sendOrBuildThrows_unexpectedErrorRow(final Failure failure, final DlqMode dlqMode) {
        final Throwable thrown = failure == Failure.SEND_STACK_OVERFLOW_ERROR
                ? new StackOverflowError("deep") : new RuntimeException("bad");
        final ParallelFlusher parallelFlusher = spy(flusher(mock(ExecutorService.class), outcomeHandlerFor(dlqMode)));
        final List<EventHandle> handles = handles(3);
        final Batch batch = batchOf("a", handles);
        if (failure == Failure.BUILD_RUNTIME_EXCEPTION) {
            doThrow(thrown).when(parallelFlusher).buildRequest(batch);
        } else {
            when(batchSender.send(any())).thenThrow(thrown);
        }

        parallelFlusher.runTask(batch);

        final boolean expected = dlqMode == DlqMode.OK;
        handles.forEach(handle -> verify(handle, times(1)).release(expected));
        handles.forEach(handle -> verify(handle, times(1)).release(anyBoolean()));
        verify(metrics, times(1)).increaseUnhandledErrorCounter(1);
        final String expectedKey = dlqMode == DlqMode.NONE ? "negativelyAcked:" + REASON_UNEXPECTED_ERROR
                : dlqMode == DlqMode.OK ? "sentToDlq:" + REASON_UNEXPECTED_ERROR : "negativelyAcked:" + REASON_DLQ_WRITE_FAILED;
        assertThat(dispositions(), equalTo(Map.of(expectedKey, 3)));
        if (dlqMode == DlqMode.NONE) {
            assertThat(performed, empty());
        } else {
            assertThat(performed.size(), equalTo(1));
            assertThat(performed.get(0).get(0).getFailedData(),
                    equalTo(CloudWatchLogsSinkDlqData.createDlqData(0, "a-0", thrown.getMessage())));
        }
    }

    @Test
    void runTask_senderInterrupted_flagClearDuringReleaseAndRestoredAfter() {
        final List<EventHandle> handles = handles(2);
        final List<Boolean> interruptedDuringRelease = new ArrayList<>();
        handles.forEach(handle -> when(handle.release(anyBoolean())).thenAnswer(invocation -> {
            interruptedDuringRelease.add(Thread.currentThread().isInterrupted());
            return true;
        }));
        when(batchSender.send(any())).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return BatchSender.Result.failed(new RuntimeException("x"));
        });

        flusher(mock(ExecutorService.class), outcomeHandlerFor(DlqMode.NONE)).runTask(batchOf("a", handles));

        assertThat(interruptedDuringRelease, equalTo(List.of(false, false)));
        assertThat(Thread.currentThread().isInterrupted(), equalTo(true));
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:" + REASON_RETRIES_EXHAUSTED, 2)));
    }

    @Test
    void runTask_outcomeHandlingThrows_sweepReleasesFalse() {
        final List<EventHandle> handles = handles(2);
        when(batchSender.send(any())).thenReturn(null);

        assertThrows(NullPointerException.class,
                () -> flusher(mock(ExecutorService.class), outcomeHandlerFor(DlqMode.OK)).runTask(batchOf("a", handles)));

        handles.forEach(handle -> verify(handle, times(1)).release(false));
        verify(dlqPushHandler, never()).perform(any());
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:" + REASON_UNEXPECTED_ERROR, 2)));
    }

    // A rejected batch has no task to own it, so the caller settles it; submitted batches are left to their task.
    @Test
    void flush_submitRejected_callerSettlesShutdownSubmittedLeftToTask() {
        final ExecutorService rejectingPool = mock(ExecutorService.class);
        when(rejectingPool.submit(any(Runnable.class)))
                .thenAnswer(invocation -> {
                    invocation.<Runnable>getArgument(0).run();
                    return CompletableFuture.completedFuture(null);
                })
                .thenThrow(new RejectedExecutionException("rejected"));
        when(batchSender.send(any())).thenReturn(delivered());
        final List<EventHandle> submitted = handles(2);
        final List<EventHandle> rejected = handles(2);
        final Batch submittedBatch = batchOf("a", submitted);
        final Batch rejectedBatch = batchOf("b", rejected);

        flusher(rejectingPool, outcomeHandlerFor(DlqMode.OK)).flush(List.of(submittedBatch, rejectedBatch));

        verify(batchSender, times(1)).send(any());
        submitted.forEach(handle -> verify(handle, times(1)).release(true));
        rejected.forEach(handle -> verify(handle, times(1)).release(true));
        assertThat(submittedBatch.isSettlementStarted(), equalTo(true));
        assertThat(rejectedBatch.isSettlementStarted(), equalTo(true));
        assertThat(dispositions(), equalTo(Map.of("succeeded", 2, "sentToDlq:" + REASON_SHUTDOWN, 2)));
        assertThat(performed.size(), equalTo(1));
        assertThat(performed.get(0).get(0).getFailedData(), equalTo(CloudWatchLogsSinkDlqData.createDlqData(0, "b-0", REASON_SHUTDOWN)));
        verify(rejectingPool, never()).shutdownNow();
    }

    @ParameterizedTest
    @MethodSource("taskThrowables")
    void flush_taskThrowsExceptionOrStackOverflow_notRethrownSweptFalseCallerTouchesNothing(final Throwable thrown) {
        pool = daemonPool(1);
        final OutcomeHandler outcomeHandler = spy(outcomeHandlerFor(DlqMode.OK));
        when(batchSender.send(any())).thenReturn(delivered());
        doThrow(thrown).when(outcomeHandler).handleResult(any(), any());
        final List<EventHandle> handles = handles(2);
        final Batch batch = batchOf("a", handles);

        flusher(pool, outcomeHandler).flush(List.of(batch));

        verify(outcomeHandler, times(1)).releaseUnreleased(batch);
        verify(outcomeHandler, never()).settle(any(), any(), any());
        handles.forEach(handle -> verify(handle, times(1)).release(false));
        verify(dlqPushHandler, never()).perform(any());
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:" + REASON_UNEXPECTED_ERROR, 2)));
    }

    private static Stream<Throwable> taskThrowables() {
        return Stream.of(new IllegalStateException("bad"), new StackOverflowError("deep"));
    }

    @Test
    void flush_taskThrowsOutOfMemoryError_sweepsFalseNoPerformRethrowsAfterAllFuturesDone() throws Exception {
        pool = daemonPool(2);
        final List<EventHandle> failing = handles(2);
        final List<EventHandle> slow = handles(2);
        final CountDownLatch failingSwept = new CountDownLatch(failing.size());
        failing.forEach(handle -> when(handle.release(anyBoolean())).thenAnswer(invocation -> {
            failingSwept.countDown();
            return true;
        }));
        final CountDownLatch slowGate = new CountDownLatch(1);
        final AtomicBoolean slowDone = new AtomicBoolean();
        when(batchSender.send(any())).thenAnswer(invocation -> {
            final PutLogEventsRequest request = invocation.getArgument(0);
            if (request.logEvents().get(0).message().startsWith("failing")) {
                throw new OutOfMemoryError("oom");
            }
            slowGate.await();
            slowDone.set(true);
            return delivered();
        });
        final ParallelFlusher parallelFlusher = flusher(pool, outcomeHandlerFor(DlqMode.OK));
        final AtomicReference<Throwable> flushThrew = new AtomicReference<>();
        final AtomicBoolean slowDoneWhenFlushThrew = new AtomicBoolean();
        final Thread caller = daemonThread(() -> {
            try {
                parallelFlusher.flush(List.of(batchOf("failing", failing), batchOf("slow", slow)));
            } catch (final Throwable t) {
                slowDoneWhenFlushThrew.set(slowDone.get());
                flushThrew.set(t);
            }
        }, "caller");
        caller.start();

        assertThat(failingSwept.await(WAIT_SECONDS, TimeUnit.SECONDS), equalTo(true));
        // Negative check: flush must not rethrow while the slow task is still running.
        caller.join(NEGATIVE_CHECK_MILLIS);
        assertThat(flushThrew.get(), nullValue());
        slowGate.countDown();
        caller.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));

        assertThat(flushThrew.get(), instanceOf(OutOfMemoryError.class));
        assertThat(slowDoneWhenFlushThrew.get(), equalTo(true));
        failing.forEach(handle -> verify(handle, times(1)).release(false));
        slow.forEach(handle -> verify(handle, times(1)).release(true));
        verify(dlqPushHandler, never()).perform(any());
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:" + REASON_UNEXPECTED_ERROR, 2, "succeeded", 2)));
    }

    @Test
    void flush_twoCallersManyBatches_inFlightAtMostWorkersNoDuplicateReturnsAfterAllReleased() throws Exception {
        final int workers = 2;
        final int batchesPerCaller = 5;
        pool = daemonPool(workers);
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        final Map<String, Integer> sends = new ConcurrentHashMap<>();
        final CountDownLatch poolSaturated = new CountDownLatch(workers);
        final CountDownLatch gate = new CountDownLatch(1);
        when(batchSender.send(any())).thenAnswer(invocation -> {
            final PutLogEventsRequest request = invocation.getArgument(0);
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            sends.merge(request.logEvents().get(0).message(), 1, Integer::sum);
            poolSaturated.countDown();
            gate.await();
            inFlight.decrementAndGet();
            return delivered();
        });
        final ParallelFlusher parallelFlusher = flusher(pool, outcomeHandlerFor(DlqMode.NONE));
        final List<Thread> callers = new ArrayList<>();
        final Map<String, Boolean> allReleasedOnReturn = new ConcurrentHashMap<>();
        for (final String caller : List.of("c0", "c1")) {
            final AtomicInteger releasedCount = new AtomicInteger();
            final List<Batch> batches = new ArrayList<>();
            for (int b = 0; b < batchesPerCaller; b++) {
                final List<EventHandle> handles = handles(3);
                handles.forEach(handle -> when(handle.release(anyBoolean())).thenAnswer(invocation -> {
                    releasedCount.incrementAndGet();
                    return true;
                }));
                batches.add(batchOf(caller + "b" + b, handles));
            }
            callers.add(daemonThread(() -> {
                parallelFlusher.flush(batches);
                allReleasedOnReturn.put(caller, releasedCount.get() == batchesPerCaller * 3);
            }, caller));
        }
        callers.forEach(Thread::start);

        assertThat(poolSaturated.await(WAIT_SECONDS, TimeUnit.SECONDS), equalTo(true));
        assertThat(allReleasedOnReturn.isEmpty(), equalTo(true));
        gate.countDown();
        for (final Thread caller : callers) {
            caller.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        }

        assertThat(maxInFlight.get(), lessThanOrEqualTo(workers));
        assertThat(sends.size(), equalTo(2 * batchesPerCaller));
        assertThat(Set.copyOf(sends.values()), equalTo(Set.of(1)));
        assertThat(allReleasedOnReturn, equalTo(Map.of("c0", true, "c1", true)));
        assertThat(dispositionTotal(), equalTo(2 * batchesPerCaller * 3));
    }

    @Test
    void flush_callerInterrupted_noCancelWaitsAllTasksSettleCallerReleasesNothingFlagSet() throws Exception {
        pool = daemonPool(2);
        final CountDownLatch gatedSendEntered = new CountDownLatch(1);
        final CountDownLatch gate = new CountDownLatch(1);
        final AtomicBoolean senderInterrupted = new AtomicBoolean();
        when(batchSender.send(any())).thenAnswer(invocation -> {
            final PutLogEventsRequest request = invocation.getArgument(0);
            if (request.logEvents().get(0).message().startsWith("gated")) {
                gatedSendEntered.countDown();
                try {
                    gate.await();
                } catch (final InterruptedException e) {
                    senderInterrupted.set(true);
                }
            }
            return BatchSender.Result.failed(new RuntimeException("x"));
        });
        final List<String> releasingThreads = Collections.synchronizedList(new ArrayList<>());
        final List<EventHandle> handles = handles(4);
        final CountDownLatch ungatedReleased = new CountDownLatch(2);
        for (int i = 0; i < handles.size(); i++) {
            final boolean ungated = i >= 2;
            when(handles.get(i).release(anyBoolean())).thenAnswer(invocation -> {
                releasingThreads.add(Thread.currentThread().getName());
                if (ungated) {
                    ungatedReleased.countDown();
                }
                return true;
            });
        }
        final ParallelFlusher parallelFlusher = flusher(pool, outcomeHandlerFor(DlqMode.OK));
        final AtomicInteger releasedOnReturn = new AtomicInteger(-1);
        final AtomicBoolean interruptedOnReturn = new AtomicBoolean();
        final Thread caller = daemonThread(() -> {
            parallelFlusher.flush(List.of(batchOf("gated", handles.subList(0, 2)), batchOf("ungated", handles.subList(2, 4))));
            releasedOnReturn.set(releasingThreads.size());
            interruptedOnReturn.set(Thread.currentThread().isInterrupted());
        }, "caller");
        caller.start();

        assertThat(gatedSendEntered.await(WAIT_SECONDS, TimeUnit.SECONDS), equalTo(true));
        assertThat(ungatedReleased.await(WAIT_SECONDS, TimeUnit.SECONDS), equalTo(true));
        caller.interrupt();
        // Negative check: a caller that stopped waiting on the interrupt would return here, its last future being done already.
        caller.join(NEGATIVE_CHECK_MILLIS);
        assertThat(caller.isAlive(), equalTo(true));
        gate.countDown();
        caller.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));

        assertThat(senderInterrupted.get(), equalTo(false));
        verify(batchSender, times(2)).send(any());
        assertThat(releasedOnReturn.get(), equalTo(4));
        assertThat(interruptedOnReturn.get(), equalTo(true));
        assertThat(releasingThreads.stream().filter(name -> !name.startsWith("flusher-pool")).collect(Collectors.toList()), empty());
        handles.forEach(handle -> verify(handle, times(1)).release(true));
        assertThat(performed.size(), equalTo(2));
        assertThat(dispositions(), equalTo(Map.of("sentToDlq:" + REASON_RETRIES_EXHAUSTED, 4)));
    }
}
