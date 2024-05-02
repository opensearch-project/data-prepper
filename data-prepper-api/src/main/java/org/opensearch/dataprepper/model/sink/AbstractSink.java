/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;

import java.util.Collection;

/**
 * This class implements the Sink interface and records boilerplate metrics
 */
public abstract class AbstractSink<T extends Record<?>> implements Sink<T> {
    protected static final int DEFAULT_MAX_RETRIES = 600;
    protected static final int DEFAULT_WAIT_TIME_MS = 1000;
    protected final PluginMetrics pluginMetrics;
    protected final PluginSetting pluginSetting;
    private final Counter recordsInCounter;
    private final SinkLatencyMetrics latencyMetrics;
    private final Timer timeElapsedTimer;
    private Thread retryThread;
    private int maxRetries;
    private int waitTimeMs;

    public AbstractSink(final PluginSetting pluginSetting, int numRetries, int waitTimeMs) {
        this.pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);
        recordsInCounter = pluginMetrics.counter(MetricNames.RECORDS_IN);
        timeElapsedTimer = pluginMetrics.timer(MetricNames.TIME_ELAPSED);
        this.latencyMetrics = new SinkLatencyMetrics(pluginMetrics);
        retryThread = null;
        this.maxRetries = numRetries;
        this.waitTimeMs = waitTimeMs;
        this.pluginSetting = pluginSetting;
    }

    public AbstractSink(final PluginSetting pluginSetting) {
        this(pluginSetting, DEFAULT_MAX_RETRIES, DEFAULT_WAIT_TIME_MS);
    }

    public abstract void doInitialize();

    @Override
    public void initialize() {
        // Derived class supposed to catch retryable exceptions and throw
        // the exceptions which are not retryable.
        doInitialize();
        if (!isReady() && retryThread == null) {
            retryThread = new Thread(new SinkThread(this, maxRetries, waitTimeMs));
            retryThread.start();
        }
    }

    /**
     * Records metrics for ingress and time elapsed, while calling
     * doOutput to perform the actual output logic
     * @param records the records to write to the sink.
     */
    @Override
    public void output(Collection<T> records) {
        recordsInCounter.increment(records.size()*1.0);
        timeElapsedTimer.record(() -> doOutput(records));
    }

    @Override
    public Object outputSync(Collection<T> records, boolean isQuery) {
        recordsInCounter.increment(records.size()*1.0);
        return timeElapsedTimer.record(() -> doOutputSync(records, isQuery));
    }

    /**
     * This method should implement the output logic
     * @param records Records to be output
     */
    public abstract void doOutput(Collection<T> records);

    public Object doOutputSync(Collection<T> records, boolean isQuery) {
        return "";
    }

    @Override
    public void shutdown() {
        if (retryThread != null) {
            retryThread.stop();
        }
    }

    @Override
    public void updateLatencyMetrics(Collection<T> records) {
        for (final Record record : records) {
            if (record.getData() instanceof Event) {
                Event event = (Event)record.getData();
                event.getEventHandle().onRelease((eventHandle, result) -> {
                    if (result) {
                        latencyMetrics.update(eventHandle);
                    }
                });
            }
        }
    }

    Thread.State getRetryThreadState() {
        if (retryThread != null) {
            return retryThread.getState();
        }
        return null;
    }
}
