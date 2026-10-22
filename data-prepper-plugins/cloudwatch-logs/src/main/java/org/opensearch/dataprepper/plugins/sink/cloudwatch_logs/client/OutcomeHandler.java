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

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.dlq.CloudWatchLogsSinkDlqData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.RejectedLogEventsInfo;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_DLQ_WRITE_FAILED;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_REJECTED;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_RETRIES_EXHAUSTED;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_SHUTDOWN;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_UNDELIVERABLE;
import static org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client.CloudWatchLogsMetrics.REASON_UNEXPECTED_ERROR;

/**
 * Settles batch outcomes and is the only place an event handle is released. Every method must be called on the thread that owns
 * the handles it is given.
 */
@RequiredArgsConstructor
public class OutcomeHandler {
    private static final Logger LOG = LoggerFactory.getLogger(OutcomeHandler.class);

    static final String TOO_OLD_FAILURE_MESSAGE = "Too old log event";

    @AllArgsConstructor
    public enum Reason {
        UNDELIVERABLE(REASON_UNDELIVERABLE, true),
        REJECTED(REASON_REJECTED, true),
        RETRIES_EXHAUSTED(REASON_RETRIES_EXHAUSTED, false),
        UNEXPECTED_ERROR(REASON_UNEXPECTED_ERROR, false),
        SHUTDOWN(REASON_SHUTDOWN, false);

        @Getter
        private final String tag;
        private final boolean releasesTrueWithoutDlq;

        /** Without a DLQ, true releases the handles with true (released without delivery) and false negatively acks them. */
        boolean releasesTrueWithoutDlq() {
            return releasesTrueWithoutDlq;
        }
    }

    /** The message is the serialized event, or null if serialization failed. */
    @AllArgsConstructor
    public static final class FailedEvent {
        private final EventHandle eventHandle;
        private final String message;
        private final String failureMessage;
    }

    private enum Disposition { SUCCEEDED, RELEASED_WITHOUT_DELIVERY, SENT_TO_DLQ, NEGATIVELY_ACKED }

    /** Null when no DLQ is configured. */
    private final DlqPushHandler dlqPushHandler;
    private final CloudWatchLogsMetrics cloudWatchLogsMetrics;

    /**
     * Writes events that share one reason to the DLQ in a single call, skipping handles already in {@code released}. If the DLQ
     * write fails the events are negatively acked so they are not lost. The interrupt flag is cleared during the DLQ write, so an
     * interrupted thread can still write, and restored afterwards.
     */
    public void terminalStep(final Set<EventHandle> released, final Reason reason, final List<FailedEvent> events) {
        final List<FailedEvent> pending = new ArrayList<>(events.size());
        for (final FailedEvent event : events) {
            if (!released.contains(event.eventHandle)) {
                pending.add(event);
            }
        }
        if (pending.isEmpty()) {
            return;
        }
        if (reason == Reason.UNEXPECTED_ERROR) {
            try {
                cloudWatchLogsMetrics.increaseUnhandledErrorCounter(1);
            } catch (final Exception e) {
                LOG.error("Failed to count an unhandled error", e);
            }
        }
        if (dlqPushHandler == null) {
            releaseWithoutDlq(released, reason, pending);
            return;
        }
        final boolean wasInterrupted = Thread.interrupted();
        try {
            final boolean written = writeToDlq(reason, pending);
            for (final FailedEvent event : pending) {
                if (written) {
                    release(released, event.eventHandle, Disposition.SENT_TO_DLQ, reason.getTag());
                } else {
                    release(released, event.eventHandle, Disposition.NEGATIVELY_ACKED, REASON_DLQ_WRITE_FAILED);
                }
            }
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void releaseWithoutDlq(final Set<EventHandle> released, final Reason reason, final List<FailedEvent> pending) {
        if (!reason.releasesTrueWithoutDlq()) {
            pending.forEach(event -> release(released, event.eventHandle, Disposition.NEGATIVELY_ACKED, reason.getTag()));
            return;
        }
        if (reason == Reason.UNDELIVERABLE) {
            try {
                cloudWatchLogsMetrics.increaseLogLargeEventsDroppedCounter(pending.size());
            } catch (final Exception e) {
                LOG.error("Failed to count dropped large events", e);
            }
        }
        pending.forEach(event -> release(released, event.eventHandle, Disposition.RELEASED_WITHOUT_DELIVERY, reason.getTag()));
    }

    private boolean writeToDlq(final Reason reason, final List<FailedEvent> pending) {
        try {
            final List<DlqObject> dlqObjects = new ArrayList<>(pending.size());
            for (final FailedEvent event : pending) {
                // CloudWatchLogsSinkDlqData rejects null data and message. An event that failed to serialize has no message, so
                // it is written with empty data.
                final String data = event.message == null ? "" : event.message;
                final String failureMessage = event.failureMessage == null ? reason.getTag() : event.failureMessage;
                dlqObjects.add(DlqObject.createDlqObject(dlqPushHandler.getPluginSetting(), List.of(event.eventHandle),
                        CloudWatchLogsSinkDlqData.createDlqData(0, data, failureMessage)));
            }
            return dlqPushHandler.perform(dlqObjects);
        } catch (final Exception e) {
            LOG.error("Failed to write {} events to the DLQ", pending.size(), e);
            return false;
        }
    }

    void handleResult(final Batch batch, final BatchSender.Result result) {
        switch (result.getStatus()) {
            case DELIVERED:
                handleDelivered(batch, result.getResponse());
                break;
            case FAILED:
                terminalStep(batch, Reason.RETRIES_EXHAUSTED, result.getLastException());
                break;
            default:
                terminalStep(batch, Reason.SHUTDOWN, result.getLastException());
                break;
        }
    }

    /** expiredLogEventEndIndex is ignored. */
    private void handleDelivered(final Batch batch, final PutLogEventsResponse response) {
        final RejectedLogEventsInfo rejectedLogEventsInfo = response == null ? null : response.rejectedLogEventsInfo();
        final Integer tooOldEndIndex = rejectedLogEventsInfo == null ? null : rejectedLogEventsInfo.tooOldLogEventEndIndex();
        final Integer tooNewStartIndex = rejectedLogEventsInfo == null ? null : rejectedLogEventsInfo.tooNewLogEventStartIndex();
        final List<EventHandle> handles = batch.getEventHandles();
        final List<FailedEvent> rejected = new ArrayList<>();
        for (int i = 0; i < handles.size(); i++) {
            final boolean isRejected = (tooOldEndIndex != null && i < tooOldEndIndex)
                    || (tooNewStartIndex != null && i >= tooNewStartIndex);
            if (isRejected) {
                rejected.add(failedEvent(batch, i, TOO_OLD_FAILURE_MESSAGE));
            } else {
                release(batch.getReleased(), handles.get(i), Disposition.SUCCEEDED, null);
            }
        }
        terminalStep(batch.getReleased(), Reason.REJECTED, rejected);
    }

    /**
     * Settles a batch on the calling thread, which must still own it. Any handle the terminal step did not release is released
     * with false.
     */
    public void settle(final Batch batch, final Reason reason, final Throwable cause) {
        batch.markSettlementStarted();
        try {
            terminalStep(batch, reason, cause);
        } finally {
            releaseUnreleased(batch);
        }
    }

    public void settleUnsettled(final List<Batch> batches, final Reason reason, final Throwable cause) {
        for (final Batch batch : batches) {
            if (!batch.isSettlementStarted()) {
                settle(batch, reason, cause);
            }
        }
    }

    void terminalStep(final Batch batch, final Reason reason, final Throwable cause) {
        final String failureMessage = failureMessage(cause, reason);
        final List<FailedEvent> events = new ArrayList<>(batch.getEventHandles().size());
        for (int i = 0; i < batch.getEventHandles().size(); i++) {
            events.add(failedEvent(batch, i, failureMessage));
        }
        terminalStep(batch.getReleased(), reason, events);
    }

    /**
     * Releases with false, without a DLQ write, every handle not already in {@code released}.
     *
     * @return the number of handles released
     */
    public int releaseUnreleased(final Set<EventHandle> released, final Collection<EventHandle> handles) {
        int count = 0;
        for (final EventHandle handle : handles) {
            if (release(released, handle, Disposition.NEGATIVELY_ACKED, REASON_UNEXPECTED_ERROR)) {
                count++;
            }
        }
        return count;
    }

    void releaseUnreleased(final Batch batch) {
        final int count = releaseUnreleased(batch.getReleased(), batch.getEventHandles());
        if (count > 0) {
            LOG.error("Released {} unsettled events of a batch with false", count);
        }
    }

    /** Releases with false, without a DLQ write, every handle of each batch whose settlement has not started. */
    public void releaseUnsettled(final List<Batch> batches) {
        for (final Batch batch : batches) {
            if (!batch.isSettlementStarted()) {
                batch.markSettlementStarted();
                releaseUnreleased(batch);
            }
        }
    }

    private static String failureMessage(final Throwable cause, final Reason reason) {
        if (cause != null && cause.getMessage() != null) {
            return cause.getMessage();
        }
        return reason.getTag();
    }

    private static FailedEvent failedEvent(final Batch batch, final int index, final String failureMessage) {
        return new FailedEvent(batch.getEventHandles().get(index), new String(batch.getMessages().get(index), StandardCharsets.UTF_8),
                failureMessage);
    }

    /**
     * The only place a handle is released and counted, so each handle is released exactly once.
     *
     * @return true if this call released the handle
     */
    private boolean release(final Set<EventHandle> released, final EventHandle handle, final Disposition disposition,
                            final String reason) {
        if (!released.add(handle)) {
            return false;
        }
        try {
            handle.release(disposition != Disposition.NEGATIVELY_ACKED);
        } catch (final Exception e) {
            LOG.error("EventHandle.release threw", e);
        }
        try {
            countDisposition(disposition, reason);
        } catch (final Exception e) {
            LOG.error("Failed to count a released event", e);
        }
        return true;
    }

    private void countDisposition(final Disposition disposition, final String reason) {
        switch (disposition) {
            case SUCCEEDED:
                cloudWatchLogsMetrics.increaseLogEventSuccessCounter(1);
                break;
            case RELEASED_WITHOUT_DELIVERY:
                cloudWatchLogsMetrics.increaseReleasedWithoutDeliveryCounter(reason, 1);
                break;
            case SENT_TO_DLQ:
                cloudWatchLogsMetrics.increaseEventsSentToDlqCounter(reason, 1);
                break;
            default:
                cloudWatchLogsMetrics.increaseEventsNegativelyAckedCounter(reason, 1);
                break;
        }
    }
}
