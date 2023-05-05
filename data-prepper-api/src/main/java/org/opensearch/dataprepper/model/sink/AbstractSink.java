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

import java.util.Collection;

/**
 * This class implements the Sink interface and records boilerplate metrics
 */
public abstract class AbstractSink<T extends Record<?>> implements Sink<T> {
    protected static final int NUMBER_OF_RETRIES = 600;
    protected final PluginMetrics pluginMetrics;
    private final Counter recordsInCounter;
    private final Timer timeElapsedTimer;
    private Thread retryThread;

    public AbstractSink(final PluginSetting pluginSetting) {
        this.pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);
        recordsInCounter = pluginMetrics.counter(MetricNames.RECORDS_IN);
        timeElapsedTimer = pluginMetrics.timer(MetricNames.TIME_ELAPSED);
        retryThread = null;
    }

    public abstract void doInitialize();

    @Override
    public void initialize() {
        // Derived class supposed to catch retryable exceptions and throw
        // the exceptions which are not retryable.
        doInitialize();
        if (!isReady() && retryThread == null) {
            retryThread = new Thread(new SinkThread(this, NUMBER_OF_RETRIES));
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

    /**
     * This method should implement the output logic
     * @param records Records to be output
     */
    public abstract void doOutput(Collection<T> records);

    @Override
    public void shutdown() {
        if (retryThread != null) {
            retryThread.stop();
        }
    }

    Thread.State getRetryThreadState() {
        if (retryThread != null) {
            return retryThread.getState();
        }
        return null;
    }
}
