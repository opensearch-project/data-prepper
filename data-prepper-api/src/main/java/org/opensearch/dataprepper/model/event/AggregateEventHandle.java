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
import java.time.Instant;
import java.io.Serializable;

public class AggregateEventHandle extends AbstractEventHandle implements Serializable {
    private List<WeakReference<AcknowledgementSet>> acknowledgementSetRefList;
    private Set<Integer> acknowledgementSetHashes;

    public AggregateEventHandle(final Instant internalOriginationTime) {
        super(internalOriginationTime);
        this.acknowledgementSetRefList = new ArrayList<>();
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
    public boolean hasAcknowledgementSet() {
        return acknowledgementSetRefList.size() != 0;
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
        notifyReleaseConsumers(result);
        boolean returnValue = true;
        synchronized (this) {
            for (WeakReference<AcknowledgementSet> acknowledgementSetRef: acknowledgementSetRefList) {
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

}

