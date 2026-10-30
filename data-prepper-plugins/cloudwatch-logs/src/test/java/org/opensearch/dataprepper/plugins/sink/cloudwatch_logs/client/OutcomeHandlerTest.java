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
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.invocation.Invocation;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.InternalEventHandle;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.dlq.CloudWatchLogsSinkDlqData;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedLogEventsInfo;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_DLQ_WRITE_FAILED;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_PERMANENT_FAILURE;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_REJECTED;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_RETRIES_EXHAUSTED;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_SHUTDOWN;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_UNDELIVERABLE;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_UNEXPECTED_ERROR;

class OutcomeHandlerTest {
    private static final int BATCH_SIZE = 4;
    private static final String LAST_EXCEPTION_MESSAGE = "last attempt failed";

    enum DlqMode { NONE, OK, FALSE, THROWS }

    enum Outcome { DELIVERED, DELIVERED_TOO_OLD, DELIVERED_TOO_NEW, FAILED, SHUTDOWN }

    private CloudWatchLogsMetrics metrics;
    private DlqPushHandler dlqPushHandler;
    private List<List<DlqObject>> performed;
    private List<Boolean> interruptedDuringPerform;

    @BeforeEach
    void setUp() {
        metrics = mock(CloudWatchLogsMetrics.class);
        dlqPushHandler = mock(DlqPushHandler.class);
        final PluginSetting pluginSetting = new PluginSetting("dlq", Map.of());
        pluginSetting.setPipelineName("pipeline");
        when(dlqPushHandler.getPluginSetting()).thenReturn(pluginSetting);
        performed = new ArrayList<>();
        interruptedDuringPerform = new ArrayList<>();
    }

    @AfterEach
    void clearInterrupt() {
        Thread.interrupted();
    }

    private OutcomeHandler handlerFor(final DlqMode dlqMode) {
        if (dlqMode == DlqMode.NONE) {
            return new OutcomeHandler(null, metrics);
        }
        doAnswer(invocation -> {
            performed.add(new ArrayList<>(invocation.getArgument(0)));
            interruptedDuringPerform.add(Thread.currentThread().isInterrupted());
            if (dlqMode == DlqMode.THROWS) {
                throw new RuntimeException("dlq writer failed");
            }
            return dlqMode == DlqMode.OK;
        }).when(dlqPushHandler).perform(any());
        return new OutcomeHandler(dlqPushHandler, metrics);
    }

    private static List<EventHandle> handles(final int count) {
        return IntStream.range(0, count).mapToObj(i -> mock(EventHandle.class)).collect(Collectors.toList());
    }

    private static String message(final int index) {
        return "message-" + index;
    }

    private static Batch batchOf(final List<EventHandle> handles) {
        final List<byte[]> messages = IntStream.range(0, handles.size())
                .mapToObj(i -> message(i).getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());
        return new Batch(handles, messages, null, null);
    }

    private static Set<EventHandle> newReleasedSet() {
        return Collections.newSetFromMap(new IdentityHashMap<>());
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

    private static Map<String, Integer> expectedTerminal(final String reason, final boolean releasesTrueWithoutDlq,
                                                         final DlqMode dlqMode, final int count) {
        final Map<String, Integer> expected = new HashMap<>();
        if (count == 0) {
            return expected;
        }
        if (dlqMode == DlqMode.NONE) {
            expected.put((releasesTrueWithoutDlq ? "releasedWithoutDelivery:" : "negativelyAcked:") + reason, count);
        } else if (dlqMode == DlqMode.OK) {
            expected.put("sentToDlq:" + reason, count);
        } else {
            expected.put("negativelyAcked:" + REASON_DLQ_WRITE_FAILED, count);
        }
        return expected;
    }

    private static boolean expectedTerminalValue(final boolean releasesTrueWithoutDlq, final DlqMode dlqMode) {
        return dlqMode == DlqMode.NONE ? releasesTrueWithoutDlq : dlqMode == DlqMode.OK;
    }

    private static Stream<Arguments> outcomeMatrix() {
        return Stream.of(Outcome.values())
                .flatMap(outcome -> Stream.of(DlqMode.values()).map(dlqMode -> arguments(outcome, dlqMode)));
    }

    private static BatchSender.Result resultFor(final Outcome outcome) {
        final RejectedLogEventsInfo rejected;
        switch (outcome) {
            case DELIVERED:
                return BatchSender.Result.delivered(PutLogEventsResponse.builder().build());
            case DELIVERED_TOO_OLD:
                // expiredLogEventEndIndex is ignored, so index 1 counts as delivered.
                rejected = RejectedLogEventsInfo.builder().tooOldLogEventEndIndex(1).expiredLogEventEndIndex(2).build();
                return BatchSender.Result.delivered(PutLogEventsResponse.builder().rejectedLogEventsInfo(rejected).build());
            case DELIVERED_TOO_NEW:
                rejected = RejectedLogEventsInfo.builder().tooNewLogEventStartIndex(BATCH_SIZE - 1).build();
                return BatchSender.Result.delivered(PutLogEventsResponse.builder().rejectedLogEventsInfo(rejected).build());
            case FAILED:
                return BatchSender.Result.failed(new RuntimeException(LAST_EXCEPTION_MESSAGE));
            default:
                return BatchSender.Result.shutdown(null);
        }
    }

    private static Set<Integer> terminalIndexes(final Outcome outcome) {
        switch (outcome) {
            case DELIVERED:
                return Set.of();
            case DELIVERED_TOO_OLD:
                return Set.of(0);
            case DELIVERED_TOO_NEW:
                return Set.of(BATCH_SIZE - 1);
            default:
                return Set.of(0, 1, 2, 3);
        }
    }

    @ParameterizedTest
    @EnumSource(DlqMode.class)
    void handleResult_failedWithPermanentError_countedAsPermanentFailure(final DlqMode dlqMode) {
        final OutcomeHandler outcomeHandler = handlerFor(dlqMode);
        final Batch batch = batchOf(handles(BATCH_SIZE));

        outcomeHandler.handleResult(batch,
                BatchSender.Result.permanentFailure(new RuntimeException(LAST_EXCEPTION_MESSAGE)));

        assertThat(dispositions(), equalTo(expectedTerminal(REASON_PERMANENT_FAILURE, false, dlqMode, BATCH_SIZE)));
    }

    @ParameterizedTest
    @MethodSource("outcomeMatrix")
    void handleResult_matrix_releasesEachHandleOncePerOutcomeAndDlqMode(final Outcome outcome, final DlqMode dlqMode) {
        final OutcomeHandler outcomeHandler = handlerFor(dlqMode);
        final List<EventHandle> handles = handles(BATCH_SIZE);
        final Batch batch = batchOf(handles);

        outcomeHandler.handleResult(batch, resultFor(outcome));

        final String reason;
        final String failureMessage;
        if (outcome == Outcome.FAILED) {
            reason = REASON_RETRIES_EXHAUSTED;
            failureMessage = LAST_EXCEPTION_MESSAGE;
        } else if (outcome == Outcome.SHUTDOWN) {
            reason = REASON_SHUTDOWN;
            failureMessage = REASON_SHUTDOWN;
        } else {
            reason = REASON_REJECTED;
            failureMessage = OutcomeHandler.TOO_OLD_FAILURE_MESSAGE;
        }
        final boolean releasesTrueWithoutDlq = reason.equals(REASON_REJECTED);
        final Set<Integer> terminal = terminalIndexes(outcome);

        for (int i = 0; i < BATCH_SIZE; i++) {
            final boolean expected = !terminal.contains(i) || expectedTerminalValue(releasesTrueWithoutDlq, dlqMode);
            verify(handles.get(i)).release(expected);
            verify(handles.get(i), times(1)).release(anyBoolean());
        }

        final Map<String, Integer> expectedCounts = expectedTerminal(reason, releasesTrueWithoutDlq, dlqMode, terminal.size());
        if (terminal.size() < BATCH_SIZE) {
            expectedCounts.put("succeeded", BATCH_SIZE - terminal.size());
        }
        assertThat(dispositions(), equalTo(expectedCounts));
        assertThat(dispositionTotal(), equalTo(BATCH_SIZE));

        if (dlqMode == DlqMode.NONE || terminal.isEmpty()) {
            assertThat(performed, empty());
            verify(dlqPushHandler, never()).perform(any());
        } else {
            assertThat(performed, hasSize(1));
            final List<Integer> ordered = terminal.stream().sorted().collect(Collectors.toList());
            assertDlqPayload(performed.get(0), ordered.stream().map(handles::get).collect(Collectors.toList()),
                    ordered.stream().map(OutcomeHandlerTest::message).collect(Collectors.toList()), failureMessage);
        }
        verify(metrics, never()).increaseUnhandledErrorCounter(anyInt());
    }

    @Test
    void mustRetryUntilDelivered_dlqConfigured_false() {
        assertThat(handlerFor(DlqMode.OK).mustRetryUntilDelivered(batchOf(handles(2))), equalTo(false));
    }

    @Test
    void mustRetryUntilDelivered_noDlqAllHandlesHaveAcknowledgementSet_false() {
        final List<EventHandle> handles = List.of(acknowledgedHandle(true), acknowledgedHandle(true));

        assertThat(handlerFor(DlqMode.NONE).mustRetryUntilDelivered(batchOf(handles)), equalTo(false));
    }

    @Test
    void mustRetryUntilDelivered_noDlqOneHandleWithoutAcknowledgementSet_true() {
        final List<EventHandle> handles = List.of(acknowledgedHandle(true), acknowledgedHandle(false));

        assertThat(handlerFor(DlqMode.NONE).mustRetryUntilDelivered(batchOf(handles)), equalTo(true));
    }

    @Test
    void mustRetryUntilDelivered_noDlqHandleNotInternal_true() {
        assertThat(handlerFor(DlqMode.NONE).mustRetryUntilDelivered(batchOf(handles(1))), equalTo(true));
    }

    private static EventHandle acknowledgedHandle(final boolean hasAcknowledgementSet) {
        final InternalEventHandle handle = mock(InternalEventHandle.class, withSettings().extraInterfaces(EventHandle.class));
        when(handle.hasAcknowledgementSet()).thenReturn(hasAcknowledgementSet);
        return (EventHandle) handle;
    }

    private static void assertDlqPayload(final List<DlqObject> dlqObjects, final List<EventHandle> expectedHandles,
                                         final List<String> expectedMessages, final String failureMessage) {
        assertThat(dlqObjects, hasSize(expectedHandles.size()));
        for (int i = 0; i < dlqObjects.size(); i++) {
            assertThat(dlqObjects.get(i).getEventHandles(), equalTo(List.of(expectedHandles.get(i))));
            assertThat(dlqObjects.get(i).getFailedData(),
                    equalTo(CloudWatchLogsSinkDlqData.createDlqData(0, expectedMessages.get(i), failureMessage)));
        }
    }

    @Test
    void terminalStep_dlqWithInterruptFlagSet_clearedDuringPerformAndRestored() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.OK);
        final Batch batch = batchOf(handles(1));
        Thread.currentThread().interrupt();

        outcomeHandler.terminalStep(batch, OutcomeHandler.Reason.SHUTDOWN, null);

        assertThat(interruptedDuringPerform, equalTo(List.of(false)));
        assertThat(Thread.currentThread().isInterrupted(), equalTo(true));
    }

    @Test
    void terminalStep_dlq_onePerformPerCallNeverEmptyPayload() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.OK);
        final List<EventHandle> handles = handles(3);
        final Set<EventHandle> released = newReleasedSet();
        released.add(handles.get(1));
        final List<OutcomeHandler.FailedEvent> events = List.of(
                new OutcomeHandler.FailedEvent(handles.get(0), "a", "too big"),
                new OutcomeHandler.FailedEvent(handles.get(1), "b", "too big"),
                new OutcomeHandler.FailedEvent(handles.get(2), "c", "too big"));

        outcomeHandler.terminalStep(released, OutcomeHandler.Reason.UNDELIVERABLE, events);

        assertThat(performed, hasSize(1));
        assertDlqPayload(performed.get(0), List.of(handles.get(0), handles.get(2)), List.of("a", "c"), "too big");
        verify(handles.get(1), never()).release(anyBoolean());

        outcomeHandler.terminalStep(released, OutcomeHandler.Reason.UNDELIVERABLE, events);
        outcomeHandler.terminalStep(released, OutcomeHandler.Reason.UNDELIVERABLE, List.of());

        assertThat(performed, hasSize(1));
        verify(handles.get(0), times(1)).release(true);
        verify(handles.get(2), times(1)).release(true);
        assertThat(dispositions(), equalTo(Map.of("sentToDlq:" + REASON_UNDELIVERABLE, 2)));
    }

    @Test
    void terminalStep_dlqEventWithoutMessage_writtenWithEmptyData() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.OK);
        final EventHandle handle = mock(EventHandle.class);

        outcomeHandler.terminalStep(newReleasedSet(), OutcomeHandler.Reason.UNEXPECTED_ERROR,
                List.of(new OutcomeHandler.FailedEvent(handle, null, null)));

        assertDlqPayload(performed.get(0), List.of(handle), List.of(""), REASON_UNEXPECTED_ERROR);
        verify(handle).release(true);
    }

    @Test
    void terminalStep_undeliverableNoDlq_releasedTrueAndLargeEventsDropped() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.NONE);
        final List<EventHandle> handles = handles(2);

        outcomeHandler.terminalStep(newReleasedSet(), OutcomeHandler.Reason.UNDELIVERABLE, List.of(
                new OutcomeHandler.FailedEvent(handles.get(0), "a", "too big"),
                new OutcomeHandler.FailedEvent(handles.get(1), "b", "too big")));

        verify(handles.get(0)).release(true);
        verify(handles.get(1)).release(true);
        verify(metrics).increaseLogLargeEventsDroppedCounter(2);
        assertThat(dispositions(), equalTo(Map.of("releasedWithoutDelivery:" + REASON_UNDELIVERABLE, 2)));
    }

    @ParameterizedTest
    @EnumSource(DlqMode.class)
    void terminalStep_unexpectedError_unhandledErrorCountedOncePerCall(final DlqMode dlqMode) {
        final OutcomeHandler outcomeHandler = handlerFor(dlqMode);
        final List<EventHandle> handles = handles(3);

        outcomeHandler.terminalStep(batchOf(handles), OutcomeHandler.Reason.UNEXPECTED_ERROR, new IllegalStateException("bad"));

        verify(metrics, times(1)).increaseUnhandledErrorCounter(1);
        final boolean expected = expectedTerminalValue(false, dlqMode);
        handles.forEach(handle -> verify(handle).release(expected));
        assertThat(dispositions(), equalTo(expectedTerminal(REASON_UNEXPECTED_ERROR, false, dlqMode, 3)));
        if (dlqMode != DlqMode.NONE) {
            assertDlqPayload(performed.get(0), handles, List.of(message(0), message(1), message(2)), "bad");
        }
    }

    @Test
    void release_oneHandleThrows_othersReleasedAndAllCounted() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.NONE);
        final List<EventHandle> handles = handles(3);
        when(handles.get(1).release(anyBoolean())).thenThrow(new RuntimeException("release failed"));

        outcomeHandler.handleResult(batchOf(handles), BatchSender.Result.delivered(PutLogEventsResponse.builder().build()));

        handles.forEach(handle -> verify(handle, times(1)).release(true));
        assertThat(dispositions(), equalTo(Map.of("succeeded", 3)));
    }

    @Test
    void release_metricThrows_releaseStillHappens() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.NONE);
        final List<EventHandle> handles = handles(3);
        doThrow(new RuntimeException("metric failed")).when(metrics).increaseEventsNegativelyAckedCounter(any(), anyInt());

        outcomeHandler.handleResult(batchOf(handles), BatchSender.Result.failed(new RuntimeException("x")));

        handles.forEach(handle -> verify(handle, times(1)).release(false));
    }

    @Test
    void terminalStep_unhandledErrorMetricThrows_eventsStillSentToDlq() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.OK);
        final List<EventHandle> handles = handles(2);
        doThrow(new IllegalStateException("metric failed")).when(metrics).increaseUnhandledErrorCounter(anyInt());

        outcomeHandler.terminalStep(batchOf(handles), OutcomeHandler.Reason.UNEXPECTED_ERROR, new RuntimeException("bad"));

        assertThat(performed, hasSize(1));
        handles.forEach(handle -> verify(handle, times(1)).release(true));
        assertThat(dispositions(), equalTo(Map.of("sentToDlq:" + REASON_UNEXPECTED_ERROR, 2)));
    }

    @Test
    void terminalStep_largeEventsDroppedMetricThrows_eventsStillReleasedTrue() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.NONE);
        final List<EventHandle> handles = handles(2);
        doThrow(new IllegalStateException("metric failed")).when(metrics).increaseLogLargeEventsDroppedCounter(anyInt());

        outcomeHandler.terminalStep(newReleasedSet(), OutcomeHandler.Reason.UNDELIVERABLE, List.of(
                new OutcomeHandler.FailedEvent(handles.get(0), "a", "too big"),
                new OutcomeHandler.FailedEvent(handles.get(1), "b", "too big")));

        handles.forEach(handle -> verify(handle, times(1)).release(true));
        assertThat(dispositions(), equalTo(Map.of("releasedWithoutDelivery:" + REASON_UNDELIVERABLE, 2)));
    }

    @Test
    void releaseUnreleased_someReleased_releasesRestFalseOnce() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.OK);
        final List<EventHandle> handles = handles(3);
        final Set<EventHandle> released = newReleasedSet();
        released.add(handles.get(0));

        final int count = outcomeHandler.releaseUnreleased(released, handles);
        final int secondCount = outcomeHandler.releaseUnreleased(released, handles);

        assertThat(count, equalTo(2));
        assertThat(secondCount, equalTo(0));
        verify(handles.get(0), never()).release(anyBoolean());
        verify(handles.get(1), times(1)).release(false);
        verify(handles.get(2), times(1)).release(false);
        verify(dlqPushHandler, never()).perform(any());
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:" + REASON_UNEXPECTED_ERROR, 2)));
    }

    @ParameterizedTest
    @EnumSource(DlqMode.class)
    void settle_callerOwnedBatch_terminalStepWithReasonAndMarked(final DlqMode dlqMode) {
        final OutcomeHandler outcomeHandler = handlerFor(dlqMode);
        final List<EventHandle> handles = handles(2);
        final Batch batch = batchOf(handles);

        outcomeHandler.settle(batch, OutcomeHandler.Reason.SHUTDOWN, null);

        assertThat(batch.isSettlementStarted(), equalTo(true));
        final boolean expected = expectedTerminalValue(false, dlqMode);
        handles.forEach(handle -> verify(handle, times(1)).release(expected));
        assertThat(dispositions(), equalTo(expectedTerminal(REASON_SHUTDOWN, false, dlqMode, 2)));
    }

    @Test
    void settle_terminalStepFails_sweepReleasesFalse() {
        final OutcomeHandler outcomeHandler = new OutcomeHandler(dlqPushHandler, metrics);
        final List<EventHandle> handles = handles(2);
        when(dlqPushHandler.perform(any())).thenThrow(new StackOverflowError("dlq writer failed"));

        assertThrows(StackOverflowError.class,
                () -> outcomeHandler.settle(batchOf(handles), OutcomeHandler.Reason.UNEXPECTED_ERROR, null));

        handles.forEach(handle -> verify(handle, times(1)).release(false));
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:" + REASON_UNEXPECTED_ERROR, 2)));
    }

    // A started batch was handed to the pool and belongs to its task, so the caller must not touch it.
    @Test
    void settleUnsettled_startedBatchSkipped_onlyUnstartedSettled() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.NONE);
        final List<EventHandle> handedOver = handles(2);
        final List<EventHandle> owned = handles(2);
        final Batch handedOverBatch = batchOf(handedOver);
        handedOverBatch.markSettlementStarted();
        final Batch ownedBatch = batchOf(owned);

        outcomeHandler.settleUnsettled(List.of(handedOverBatch, ownedBatch), OutcomeHandler.Reason.UNEXPECTED_ERROR,
                new RuntimeException("boom"));

        assertThat(ownedBatch.isSettlementStarted(), equalTo(true));
        handedOver.forEach(handle -> verify(handle, never()).release(anyBoolean()));
        owned.forEach(handle -> verify(handle, times(1)).release(false));
        verify(metrics, times(1)).increaseUnhandledErrorCounter(1);
    }

    @Test
    void releaseUnsettled_startedAndUnstarted_onlyUnstartedReleasedFalseNoDlq() {
        final OutcomeHandler outcomeHandler = handlerFor(DlqMode.OK);
        final List<EventHandle> handedOver = handles(1);
        final List<EventHandle> owned = handles(2);
        final Batch handedOverBatch = batchOf(handedOver);
        handedOverBatch.markSettlementStarted();
        final Batch ownedBatch = batchOf(owned);

        outcomeHandler.releaseUnsettled(List.of(handedOverBatch, ownedBatch));

        assertThat(ownedBatch.isSettlementStarted(), equalTo(true));
        verify(handedOver.get(0), never()).release(anyBoolean());
        owned.forEach(handle -> verify(handle, times(1)).release(false));
        verify(dlqPushHandler, never()).perform(any());
        assertThat(dispositions(), equalTo(Map.of("negativelyAcked:" + REASON_UNEXPECTED_ERROR, 2)));
    }

}
