/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.event.AggregateEventHandle;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.InternalEventHandle;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.Event;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class AggregateGroup implements AggregateActionInput {
    private final GroupState groupState;
    private Instant groupStart;
    private final Lock concludeGroupLock;
    private final Lock handleEventForGroupLock;
    private final Map<Object, Object> identificationKeys;
    private Function<Duration, Boolean> customShouldConclude;
    private EventHandle eventHandle;

    AggregateGroup(final Map<Object, Object> identificationKeys) {
        this.groupState = new DefaultGroupState();
        this.identificationKeys = identificationKeys;
        this.groupStart = Instant.now();
        this.concludeGroupLock = new ReentrantLock();
        this.handleEventForGroupLock = new ReentrantLock();
        this.eventHandle = new AggregateEventHandle(Instant.now());
    }

    @Override
    public EventHandle getEventHandle() {
        return eventHandle;
    }

    public void attachToEventAcknowledgementSet(Event event) {
        InternalEventHandle internalEventHandle;
        EventHandle handle = event.getEventHandle();
        internalEventHandle = (InternalEventHandle)(handle);
        internalEventHandle.addEventHandle(eventHandle);
    }

    public GroupState getGroupState() {
        return groupState;
    }

    public Map<Object, Object> getIdentificationKeys() {
        return identificationKeys;
    }

    Instant getGroupStart() {
        return groupStart;
    }

    Lock getConcludeGroupLock() {
        return concludeGroupLock;
    }

    @Override
    public void setCustomShouldConclude(Function<Duration, Boolean> shouldConclude) {
        customShouldConclude = shouldConclude;
    }

    Lock getHandleEventForGroupLock() {
        return handleEventForGroupLock;
    }

    boolean shouldConcludeGroup(final Duration groupDuration) {
        if (customShouldConclude != null) {
            return customShouldConclude.apply(groupDuration);
        }
        return Duration.between(groupStart, Instant.now()).compareTo(groupDuration) >= 0;
    }

    void resetGroup() {
        groupStart = Instant.now();
        groupState.clear();
        this.eventHandle = new AggregateEventHandle(groupStart);
    }
}
