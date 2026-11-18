/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import lombok.AccessLevel;
import lombok.Getter;
import org.opensearch.dataprepper.model.event.EventHandle;
import software.amazon.awssdk.services.cloudwatchlogs.model.Entity;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An immutable snapshot taken from one group, sent as one PutLogEvents request. It also carries the per-batch set of
 * released handles, which only the batch's owner may touch.
 */
@Getter
public final class Batch {
    private final List<EventHandle> eventHandles;
    private final List<byte[]> messages;
    private final Entity entity;
    private final Map<String, String> headers;
    /** Identity-keyed so equal but distinct handles are each released once. Owner thread only. */
    @Getter(AccessLevel.PACKAGE)
    private final Set<EventHandle> released = Collections.newSetFromMap(new IdentityHashMap<>());
    /** Whether the caller handed the batch to the pool or started settling it. Caller thread only. */
    @Getter(AccessLevel.PACKAGE)
    private boolean settlementStarted;

    /**
     * Copies both lists. They must have the same size and at least one event. The entity and headers may be null.
     */
    public Batch(final List<EventHandle> eventHandles, final List<byte[]> messages, final Entity entity,
                 final Map<String, String> headers) {
        this.eventHandles = List.copyOf(eventHandles);
        this.messages = List.copyOf(messages);
        if (this.eventHandles.isEmpty() || this.eventHandles.size() != this.messages.size()) {
            throw new IllegalArgumentException("A batch needs at least one event and one message per event handle, got "
                    + this.eventHandles.size() + " handles and " + this.messages.size() + " messages");
        }
        this.entity = entity;
        this.headers = headers;
    }

    void markSettlementStarted() {
        settlementStarted = true;
    }
}
