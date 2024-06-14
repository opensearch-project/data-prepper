/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import java.lang.ref.WeakReference;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.time.Instant;
import java.io.Serializable;

public class DefaultEventHandle extends AbstractEventHandle implements Serializable {
    private WeakReference<AcknowledgementSet> acknowledgementSetRef;

    public DefaultEventHandle(final Instant internalOriginationTime) {
        super(internalOriginationTime);
        this.acknowledgementSetRef = null;
    }

    @Override
    public void addAcknowledgementSet(final AcknowledgementSet acknowledgementSet) {
        this.acknowledgementSetRef = new WeakReference<>(acknowledgementSet);
    }

    public AcknowledgementSet getAcknowledgementSet() {
        if (acknowledgementSetRef == null) {
            return null;
        }
        return acknowledgementSetRef.get();
    }

    @Override
    public boolean hasAcknowledgementSet() {
        AcknowledgementSet acknowledgementSet = getAcknowledgementSet();
        return acknowledgementSet != null;
    }

    @Override
    public void acquireReference() {
        synchronized (this) {
            AcknowledgementSet acknowledgementSet = getAcknowledgementSet();
            if (acknowledgementSet != null) {
                acknowledgementSet.acquire(this);
            }
        }
    }

    @Override
    public boolean release(boolean result) {
        notifyReleaseConsumers(result);
        AcknowledgementSet acknowledgementSet = getAcknowledgementSet();
        if (acknowledgementSet != null) {
            acknowledgementSet.release(this, result);
            return true;
        }
        return false;
    }

}
