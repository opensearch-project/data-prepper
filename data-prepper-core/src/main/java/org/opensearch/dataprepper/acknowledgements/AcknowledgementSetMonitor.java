/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * AcknowledgementSetMonitor - monitors the acknowledgement sets for completion/expiration
 * <p>
 * Every acknowledgement set must complete (ie get acknowledgements from all the events in it)
 * by a specified time. If it is not completed, then it is considered 'expired' and it is
 * cleaned up. The 'run' method is invoked periodically to cleanup the acknowledgement sets
 * that are either completed or expired.
 */
class AcknowledgementSetMonitor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgementSetMonitor.class);
    private final Set<AcknowledgementSet> acknowledgementSets;
    private final ReentrantLock lock;
    private final AtomicInteger numInvalidAcquires;
    private final AtomicInteger numInvalidReleases;
    private final AtomicInteger numNullHandles;

    private DefaultAcknowledgementSet getAcknowledgementSet(final EventHandle eventHandle) {
        return (DefaultAcknowledgementSet)((DefaultEventHandle)eventHandle).getAcknowledgementSet();
    }

    public AcknowledgementSetMonitor() {
        this.acknowledgementSets = new HashSet<>();
        this.lock = new ReentrantLock(true);
        this.numInvalidAcquires = new AtomicInteger(0);
        this.numInvalidReleases = new AtomicInteger(0);
        this.numNullHandles = new AtomicInteger(0);
    }

    public int getNumInvalidAcquires() {
        return numInvalidAcquires.get();
    }

    public int getNumInvalidReleases() {
        return numInvalidReleases.get();
    }

    public void add(final AcknowledgementSet acknowledgementSet) {
        lock.lock();
        try {
            acknowledgementSets.add(acknowledgementSet);
        } finally {
            lock.unlock();
        }
    }

    public void acquire(final EventHandle eventHandle) {
        if (eventHandle == null) {
            numNullHandles.incrementAndGet();
            return;
        }

        DefaultAcknowledgementSet acknowledgementSet = getAcknowledgementSet(eventHandle);
        lock.lock();
        boolean exists = false;
        try {
            exists = acknowledgementSets.contains(acknowledgementSet);
        } finally {
            lock.unlock();
        }
        // if acknowledgementSet doesn't exist then it means that the
        // event still active even after the acknowledgement set is
        // cleaned up.
        if (exists) {
            acknowledgementSet.acquire(eventHandle);
        } else {
            LOG.warn("Trying acquire an event in an AcknowledgementSet that does not exist");
            numInvalidAcquires.incrementAndGet();
        }
    }

    public void release(final EventHandle eventHandle, final boolean success) {
        if (eventHandle == null) {
            numNullHandles.incrementAndGet();
            return;
        }
        DefaultAcknowledgementSet acknowledgementSet = getAcknowledgementSet(eventHandle);
        lock.lock();
        boolean exists = false;
        try {
            exists = acknowledgementSets.contains(acknowledgementSet);
        } finally {
            lock.unlock();
        }
        // if acknowledgementSet doesn't exist then it means some late
        // arrival of event handle release after the acknowledgement set
        // is cleaned up.
        if (exists) {
            boolean b = acknowledgementSet.release(eventHandle, success);
        } else {
            LOG.warn("Trying to release from an AcknowledgementSet that does not exist");
            numInvalidReleases.incrementAndGet();
        }
    }

    /**
     * for testing
     * @return the size
     */
    int getSize() {
        return acknowledgementSets.size();
    }

    @Override
    public void run() {
        lock.lock();
        try {
            if (acknowledgementSets.size() > 0) {
                acknowledgementSets.removeIf((ackSet) -> ((DefaultAcknowledgementSet) ackSet).isDone());
            }
        } finally {
            lock.unlock();
        }
    }
}
