/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import java.lang.ref.WeakReference;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.time.Instant;
import java.io.Serializable;

public class AggregateEventHandle implements EventHandle, InternalEventHandle, Serializable {
    private Instant externalOriginationTime;
    private final Instant internalOriginationTime;
    private List<WeakReference<AcknowledgementSet>> acknowledgementSetRefList;
    private Set<Integer> acknowledgementSetHashes;
    private final ReentrantLock lock;
    private List<BiConsumer<EventHandle, Boolean>> releaseConsumers;

    public AggregateEventHandle(final Instant internalOriginationTime) {
        this.acknowledgementSetRefList = new ArrayList<>();
        this.externalOriginationTime = null;
        this.internalOriginationTime = internalOriginationTime;
        this.lock = new ReentrantLock(true);
        this.releaseConsumers = new ArrayList<>();
        this.acknowledgementSetHashes = new HashSet<>();
    }

    @Override
    public void addAcknowledgementSet(final AcknowledgementSet acknowledgementSet) {
        int hashCode = acknowledgementSet.hashCode();
        if (!acknowledgementSetHashes.contains(hashCode)) {
            this.acknowledgementSetRefList.add(new WeakReference<>(acknowledgementSet));
            acknowledgementSetHashes.add(hashCode);
        }
    }

    @Override
    public void setExternalOriginationTime(final Instant externalOriginationTime) {
        this.externalOriginationTime = externalOriginationTime;
    }

    @Override
    public boolean hasAcknowledgementSet() {
        return acknowledgementSetRefList.size() != 0;
    }

    @Override
    public Instant getInternalOriginationTime() {
        return this.internalOriginationTime;
    }

    @Override
    public Instant getExternalOriginationTime() {
        return this.externalOriginationTime;
    }

    @Override
    public void acquireReference() {
        synchronized (this) {
            for (WeakReference<AcknowledgementSet> acknowledgementSetRef: acknowledgementSetRefList) {;
                AcknowledgementSet acknowledgementSet = acknowledgementSetRef.get();
                if (acknowledgementSet != null) {
                    acknowledgementSet.acquire(this);
                } 
            }
        }
    }

    @Override
    public boolean release(boolean result) {
        synchronized (releaseConsumers) {
            for (final BiConsumer<EventHandle, Boolean> consumer: releaseConsumers) {
                consumer.accept(this, result);
            }
        }
        boolean returnValue = true;
        synchronized (this) {
            for (WeakReference<AcknowledgementSet> acknowledgementSetRef: acknowledgementSetRefList) {;
                AcknowledgementSet acknowledgementSet = acknowledgementSetRef.get();
                if (acknowledgementSet != null) {
                    acknowledgementSet.release(this, result);
                } else {
                    returnValue = false;
                }
            }
        }
        return returnValue;
    }

    // For testing
    List<WeakReference<AcknowledgementSet>> getAcknowledgementSetRefs() {
        return acknowledgementSetRefList;
    }

    @Override
    public void onRelease(BiConsumer<EventHandle, Boolean> releaseConsumer) {
        synchronized (releaseConsumers) {
            releaseConsumers.add(releaseConsumer);
        }
    }
}

