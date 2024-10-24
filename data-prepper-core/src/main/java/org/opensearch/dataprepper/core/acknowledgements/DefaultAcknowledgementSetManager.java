/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.acknowledgements;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

@Named
public class DefaultAcknowledgementSetManager implements AcknowledgementSetManager {
    private static final int DEFAULT_WAIT_TIME_MS = 15 * 1000;
    private final AcknowledgementSetMonitor acknowledgementSetMonitor;
    private final ScheduledExecutorService scheduledExecutor;
    private final AcknowledgementSetMonitorThread acknowledgementSetMonitorThread;
    private PluginMetrics pluginMetrics;
    private DefaultAcknowledgementSetMetrics metrics;

    @Inject
    public DefaultAcknowledgementSetManager(
            @Named("acknowledgementCallbackExecutor") final ScheduledExecutorService callbackExecutor) {
        this(callbackExecutor, Duration.ofMillis(DEFAULT_WAIT_TIME_MS));
    }

    public DefaultAcknowledgementSetManager(final ScheduledExecutorService callbackExecutor, final Duration waitTime) {
        this.acknowledgementSetMonitor = new AcknowledgementSetMonitor();
        this.scheduledExecutor = Objects.requireNonNull(callbackExecutor);
        acknowledgementSetMonitorThread = new AcknowledgementSetMonitorThread(acknowledgementSetMonitor, waitTime);
        acknowledgementSetMonitorThread.start();
        pluginMetrics = PluginMetrics.fromNames("acknowledgementSetManager", "acknowledgements");
        metrics = new DefaultAcknowledgementSetMetrics(pluginMetrics);
    }

    public AcknowledgementSet create(final Consumer<Boolean> callback, final Duration timeout) {
        AcknowledgementSet acknowledgementSet = new DefaultAcknowledgementSet(scheduledExecutor, callback, timeout, metrics);
        acknowledgementSetMonitor.add(acknowledgementSet);
        metrics.increment(DefaultAcknowledgementSetMetrics.CREATED_METRIC_NAME);
        return acknowledgementSet;
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
