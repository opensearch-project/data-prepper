/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import java.util.concurrent.locks.ReentrantLock;

public class ReentrantLockStrategy implements LockStrategy {
    private final ReentrantLock reentrantLock;

    public ReentrantLockStrategy() {
        reentrantLock = new ReentrantLock();
    }

    @Override
    public void lock() {
        reentrantLock.lock();
    }

    @Override
    public void unlock() {
        reentrantLock.unlock();
    }
}