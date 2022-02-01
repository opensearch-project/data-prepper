/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.locks.Lock;

class AggregateActionSynchronizer {
    private final AggregateAction aggregateAction;
    private final AggregateGroupManager aggregateGroupManager;

    private static final Logger LOG = LoggerFactory.getLogger(AggregateActionSynchronizer.class);

    private AggregateActionSynchronizer(final AggregateAction aggregateAction, final AggregateGroupManager aggregateGroupManager) {
        this.aggregateAction = aggregateAction;
        this.aggregateGroupManager = aggregateGroupManager;
    }

    Optional<Event> concludeGroup(final AggregateIdentificationKeysHasher.IdentificationHash hash, final AggregateGroup aggregateGroup) {
        final Lock concludeGroupLock = aggregateGroup.getConcludeGroupLock();
        final Lock handleEventForGroupLock = aggregateGroup.getHandleEventForGroupLock();

        Optional<Event> concludeGroupEvent = Optional.empty();
        if (concludeGroupLock.tryLock()) {
            try {
                handleEventForGroupLock.lock();
                concludeGroupEvent = aggregateAction.concludeGroup(aggregateGroup);
                aggregateGroupManager.removeGroupWithHash(hash, aggregateGroup);
                aggregateGroup.resetGroupStart();
                aggregateGroup.clearGroupState();
            } catch (Exception e) {
                LOG.debug("Error while concluding group: ", e);
            } finally{
                handleEventForGroupLock.unlock();
                concludeGroupLock.unlock();
            }
        }
        return concludeGroupEvent;
    }

    AggregateActionResponse handleEventForGroup(final Event event, final AggregateIdentificationKeysHasher.IdentificationHash hash, final AggregateGroup aggregateGroup) {
        final Lock concludeGroupLock = aggregateGroup.getConcludeGroupLock();
        final Lock handleEventForGroupLock = aggregateGroup.getHandleEventForGroupLock();

        concludeGroupLock.lock();
        concludeGroupLock.unlock();

        AggregateActionResponse handleEventResponse;
        try {
            handleEventForGroupLock.lock();
            handleEventResponse = aggregateAction.handleEvent(event, aggregateGroup);
            aggregateGroupManager.putGroupWithHash(hash, aggregateGroup);
        } catch (final Exception e) {
            LOG.debug("Error while handling event, event will be processed by remainder of the pipeline: ", e);
            handleEventResponse = new AggregateActionResponse(event);
        } finally{
            handleEventForGroupLock.unlock();
        }

        return handleEventResponse;
    }

    static class AggregateActionSynchronizerProvider {
        public AggregateActionSynchronizer provide(final AggregateAction aggregateAction, final AggregateGroupManager aggregateGroupManager) {
            return new AggregateActionSynchronizer(aggregateAction, aggregateGroupManager);
        }
    }
}
