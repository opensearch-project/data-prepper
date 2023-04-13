/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

@Named
public class DefaultAcknowledgementSetManager implements AcknowledgementSetManager {
    private static final int DEFAULT_WAIT_TIME_MS = 15 * 1000;
    private final AcknowledgementSetMonitor acknowledgementSetMonitor;
    private final ExecutorService executor;
    private final AcknowledgementSetMonitorThread acknowledgementSetMonitorThread;


    @Inject
    public DefaultAcknowledgementSetManager(
            @Named("acknowledgementCallbackExecutor") final ExecutorService callbackExecutor) {
        this(callbackExecutor, Duration.ofMillis(DEFAULT_WAIT_TIME_MS));
    }

    public DefaultAcknowledgementSetManager(final ExecutorService callbackExecutor, final Duration waitTime) {
        this.acknowledgementSetMonitor = new AcknowledgementSetMonitor();
        this.executor = Objects.requireNonNull(callbackExecutor);
        acknowledgementSetMonitorThread = new AcknowledgementSetMonitorThread(acknowledgementSetMonitor, waitTime);
        acknowledgementSetMonitorThread.start();
    }

    public AcknowledgementSet create(final Consumer<Boolean> callback, final Duration timeout) {
        AcknowledgementSet acknowledgementSet = new DefaultAcknowledgementSet(executor, callback, timeout);
        acknowledgementSetMonitor.add(acknowledgementSet);
        return acknowledgementSet;
    }

    public void acquireEventReference(final Event event) {
        acquireEventReference(event.getEventHandle());
    }

    public void acquireEventReference(final EventHandle eventHandle) {
        acknowledgementSetMonitor.acquire(eventHandle);
    }

    public void releaseEventReference(final EventHandle eventHandle, final boolean success) {
        acknowledgementSetMonitor.release(eventHandle, success);
    }

    public void shutdown() {
        acknowledgementSetMonitorThread.stop();
    }

    /**
     * For testing only.
     *
     * @return the AcknowledgementSetMonitor
     */
    AcknowledgementSetMonitor getAcknowledgementSetMonitor() {
        return acknowledgementSetMonitor;
    }
}
