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

public class DefaultEventHandle implements EventHandle, InternalEventHandle, Serializable {
    private Instant externalOriginationTime;
    private final Instant internalOriginationTime;
    private WeakReference<AcknowledgementSet> acknowledgementSetRef;
    private List<BiConsumer<EventHandle, Boolean>> releaseConsumers;

    public DefaultEventHandle(final Instant internalOriginationTime) {
        this.acknowledgementSetRef = null;
        this.externalOriginationTime = null;
        this.internalOriginationTime = internalOriginationTime;
        this.releaseConsumers = new ArrayList<>();
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
        for (final BiConsumer<EventHandle, Boolean> consumer: releaseConsumers) {
            consumer.accept(this, result);
        }
        AcknowledgementSet acknowledgementSet = getAcknowledgementSet();
        if (acknowledgementSet != null) {
            acknowledgementSet.release(this, result);
        }
    }

    @Override
    public void onRelease(BiConsumer<EventHandle, Boolean> releaseConsumer) {
        releaseConsumers.add(releaseConsumer);
    }
}
