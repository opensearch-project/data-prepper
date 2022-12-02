/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class AggregateGroup implements AggregateActionInput {
    private final GroupState groupState;
    private Instant groupStart;
    private final Lock concludeGroupLock;
    private final Lock handleEventForGroupLock;
    private final Map<Object, Object> identificationKeyMap;

    AggregateGroup(final Map<Object, Object> identificationKeyMap) {
        this.groupState = new DefaultGroupState();
        this.identificationKeyMap = identificationKeyMap;
        this.groupStart = Instant.now();
        this.concludeGroupLock = new ReentrantLock();
        this.handleEventForGroupLock = new ReentrantLock();
    }

    public GroupState getGroupState() {
        return groupState;
    }

    public Map<Object, Object> getIdentificationKeysMap() {
        return identificationKeyMap;
    }

    Instant getGroupStart() {
        return groupStart;
    }

    Map<Object, Object> getIdentificationKeyMap() {
        return identificationKeyMap;
    }

    Lock getConcludeGroupLock() {
        return concludeGroupLock;
    }

    Lock getHandleEventForGroupLock() {
        return handleEventForGroupLock;
    }

    boolean shouldConcludeGroup(final Duration groupDuration) {
        return Duration.between(groupStart, Instant.now()).compareTo(groupDuration) >= 0;
    }

    void resetGroup() {
        groupStart = Instant.now();
        groupState.clear();
    }
}
