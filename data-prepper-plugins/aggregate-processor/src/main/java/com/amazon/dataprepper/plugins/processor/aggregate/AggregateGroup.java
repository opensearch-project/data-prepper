/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import java.time.Instant;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class AggregateGroup implements AggregateActionInput {
    private final GroupState groupState;
    private Instant groupStart;
    private final Lock concludeGroupLock;
    private final Lock handleEventForGroupLock;


    AggregateGroup() {
        this.groupState = new DefaultGroupState();
        this.groupStart = Instant.now();
        this.concludeGroupLock = new ReentrantLock();
        this.handleEventForGroupLock = new ReentrantLock();
    }

    public GroupState getGroupState() {
        return groupState;
    }

    Instant getGroupStart() {
        return groupStart;
    }

    Lock getConcludeGroupLock() {
        return concludeGroupLock;
    }

    Lock getHandleEventForGroupLock() {
        return handleEventForGroupLock;
    }

    void resetGroup() {
        groupStart = Instant.now();
        groupState.clear();
    }
}
