/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import java.lang.ref.WeakReference;

import java.time.Instant;
import java.io.Serializable;

public class DefaultEventHandle implements EventHandle, Serializable {
    private Instant externalOriginationTime;
    private final Instant internalOriginationTime;
    private WeakReference<AcknowledgementSet> acknowledgementSetRef;

    public DefaultEventHandle(final Instant internalOriginationTime) {
        this.acknowledgementSetRef = null;
        this.externalOriginationTime = null;
        this.internalOriginationTime = internalOriginationTime;
    }

    @Override
    public void setAcknowledgementSet(final AcknowledgementSet acknowledgementSet) {
        this.acknowledgementSetRef = new WeakReference<>(acknowledgementSet);
    }

    @Override
    public void setExternalOriginationTime(final Instant externalOriginationTime) {
        this.externalOriginationTime = externalOriginationTime;
    }

    public AcknowledgementSet getAcknowledgementSet() {
        if (acknowledgementSetRef == null) {
            return null;
        }
        return acknowledgementSetRef.get();
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
    public void release(boolean result) {
        System.out.println("======release called==="+result);
        AcknowledgementSet acknowledgementSet = getAcknowledgementSet();
        if (acknowledgementSet != null) {
            acknowledgementSet.release(this, result);
        }
    }
}
