/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import java.util.ArrayList;
import java.util.List;
import java.time.Instant;
import java.util.function.BiConsumer;

abstract class AbstractEventHandle implements EventHandle, InternalEventHandle {
    private Instant externalOriginationTime;
    private final Instant internalOriginationTime;
    private List<BiConsumer<EventHandle, Boolean>> releaseConsumers;

    AbstractEventHandle(final Instant internalOriginationTime) {
        this.externalOriginationTime = null;
        this.internalOriginationTime = internalOriginationTime;
        this.releaseConsumers = new ArrayList<>();
    }
    @Override
    public void setExternalOriginationTime(final Instant externalOriginationTime) {
        this.externalOriginationTime = externalOriginationTime;
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
    public void onRelease(BiConsumer<EventHandle, Boolean> releaseConsumer) {
        synchronized (releaseConsumers) {
            releaseConsumers.add(releaseConsumer);
        }
    }

    public void notifyReleaseConsumers(boolean result) {
        synchronized (releaseConsumers) {
            for (final BiConsumer<EventHandle, Boolean> consumer: releaseConsumers) {
                consumer.accept(this, result);
            }
        }
    }
}
