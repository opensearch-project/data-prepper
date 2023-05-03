/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.Collections;

/**
 * An {@link AggregateAction} contains two functons, {@link AggregateAction#concludeGroup(AggregateActionInput)} and {@link AggregateAction#handleEvent(Event, AggregateActionInput)},
 * that potentially modify a shared state that needs to be synchronized between multiple worker threads. These two functions should not be called on the same {@link AggregateGroup} at the same time,
 * and this class enforces that behavior using the two {@link java.util.concurrent.locks.ReentrantLock} that belong to each {@link AggregateGroup}.
 * The synchronization is designed to hold the following conditions:
 *
 * <ol>
 *     <li>The critical sections of concludeGroup and handleEventForGroup should not be entered at the same time</li>
 *     <li>If a thread is trying to enter the critical section for concludeGroup, no new threads should be able to attempt to enter the critical section for handleEventForGroup.
 *         This condition is achieved using a turnstile synchronization pattern that locks and then immediately unlocks the concludeGroupLock</li>
 *     <li>If multiple threads try to conclude the same {@link AggregateGroup} at the same time, only one should gain access to the critical section for concludeGroup, and
 *     the remaining threads should immediately return from concludeGroup</li>
 * </ol>
 * @since 1.3
 */
class AggregateActionSynchronizer {
    static final String ACTION_HANDLE_EVENTS_PROCESSING_ERRORS = "actionHandleEventsProcessingErrors";
    static final String ACTION_CONCLUDE_GROUP_EVENTS_PROCESSING_ERRORS = "actionConcludeGroupProcessingErrors";

    private final Counter actionHandleEventsProcessingErrors;
    private final Counter actionConcludeGroupEventsProcessingErrors;

    private final AggregateAction aggregateAction;
    private final AggregateGroupManager aggregateGroupManager;

    private static final Logger LOG = LoggerFactory.getLogger(AggregateActionSynchronizer.class);

    private AggregateActionSynchronizer(final AggregateAction aggregateAction, final AggregateGroupManager aggregateGroupManager, final PluginMetrics pluginMetrics) {
        this.aggregateAction = aggregateAction;
        this.aggregateGroupManager = aggregateGroupManager;

        this.actionHandleEventsProcessingErrors = pluginMetrics.counter(ACTION_HANDLE_EVENTS_PROCESSING_ERRORS);
        this.actionConcludeGroupEventsProcessingErrors = pluginMetrics.counter(ACTION_CONCLUDE_GROUP_EVENTS_PROCESSING_ERRORS);
    }

    AggregateActionOutput concludeGroup(final AggregateIdentificationKeysHasher.IdentificationKeysMap hash, final AggregateGroup aggregateGroup, final boolean forceConclude) {
        final Lock concludeGroupLock = aggregateGroup.getConcludeGroupLock();
        final Lock handleEventForGroupLock = aggregateGroup.getHandleEventForGroupLock();

        AggregateActionOutput actionOutput = new AggregateActionOutput(Collections.emptyList());
        if (concludeGroupLock.tryLock()) {
            handleEventForGroupLock.lock();

            try {
                if (aggregateGroup.shouldConcludeGroup(aggregateGroupManager.getGroupDuration()) || forceConclude) {
                    LOG.debug("Start critical section in concludeGroup");
                    actionOutput = aggregateAction.concludeGroup(aggregateGroup);
                    aggregateGroupManager.closeGroup(hash, aggregateGroup);
                }
            } catch (final Exception e) {
                LOG.debug("Error while concluding group: ", e);
                actionConcludeGroupEventsProcessingErrors.increment();
            } finally {
                handleEventForGroupLock.unlock();
                concludeGroupLock.unlock();
            }
        }
        return actionOutput;
    }

    AggregateActionResponse handleEventForGroup(final Event event, final AggregateIdentificationKeysHasher.IdentificationKeysMap hash, final AggregateGroup aggregateGroup) {
        final Lock concludeGroupLock = aggregateGroup.getConcludeGroupLock();
        final Lock handleEventForGroupLock = aggregateGroup.getHandleEventForGroupLock();

        concludeGroupLock.lock();
        concludeGroupLock.unlock();

        AggregateActionResponse handleEventResponse;
        handleEventForGroupLock.lock();
        try {
            LOG.debug("Start critical section in handleEventForGroup");
            handleEventResponse = aggregateAction.handleEvent(event, aggregateGroup);
            aggregateGroupManager.putGroupWithHash(hash, aggregateGroup);
        } catch (final Exception e) {
            LOG.debug("Error while handling event, event will be processed by remainder of the pipeline: ", e);
            actionHandleEventsProcessingErrors.increment();
            handleEventResponse = new AggregateActionResponse(event);
        } finally {
            handleEventForGroupLock.unlock();
        }

        return handleEventResponse;
    }

    static class AggregateActionSynchronizerProvider {
        public AggregateActionSynchronizer provide(final AggregateAction aggregateAction, final AggregateGroupManager aggregateGroupManager, final PluginMetrics pluginMetrics) {
            return new AggregateActionSynchronizer(aggregateAction, aggregateGroupManager, pluginMetrics);
        }
    }
}
