/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;

import java.util.function.Consumer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

public class DefaultAcknowledgementSetManager implements AcknowledgementSetManager {
    private static final int MAX_THREADS = 100;
    private static final int DEFAULT_WAIT_TIME_MS = 15 * 1000;
    private final AcknowledgementSetMonitor acknowledgementSetMonitor;
    private final ScheduledExecutorService executor;

    public DefaultAcknowledgementSetManager() {
        this(Duration.ofMillis(DEFAULT_WAIT_TIME_MS));
    }

    public DefaultAcknowledgementSetManager(final Duration waitTime) {
        this.executor = Executors.newScheduledThreadPool(MAX_THREADS);
        this.acknowledgementSetMonitor = new AcknowledgementSetMonitor();
        this.executor.scheduleAtFixedRate(this.acknowledgementSetMonitor, waitTime.toMillis(), waitTime.toMillis(), TimeUnit.MILLISECONDS);
    }

    public AcknowledgementSet create(final Consumer<Boolean> callback, final Duration timeout) {
        AcknowledgementSet acknowledgementSet = new DefaultAcknowledgementSet(executor, callback, timeout);
        acknowledgementSetMonitor.add(acknowledgementSet);
        return acknowledgementSet;
    }

    public void acquireEventReference(final Event event) {
        acquireEventReference(((JacksonEvent)event).getEventHandle());
    }

    public void acquireEventReference(final EventHandle eventHandle) {
        acknowledgementSetMonitor.acquire(eventHandle);
    }

    public void releaseEventReference(final EventHandle eventHandle, final boolean success) {
        acknowledgementSetMonitor.release(eventHandle, success);
    }

    public void shutdown() {
        this.executor.shutdownNow();
    }

    // for testing
    public AcknowledgementSetMonitor getAcknowledgementSetMonitor() {
        return acknowledgementSetMonitor;
    }
}
