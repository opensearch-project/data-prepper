/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.record.Record;
import java.util.Collection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

/**
 * This class implements the Sink interface and records boilerplate metrics
 */
public abstract class AbstractSink<T extends Record<?>> implements Sink<T> {
    static private final String NUM_RETRIES = "num_retries";
    static private final int NUM_DEFAULT_RETRIES = 600;
    protected final PluginMetrics pluginMetrics;
    private final Counter recordsInCounter;
    private final Timer timeElapsedTimer;
    private final Thread retryThread;

    public AbstractSink(final PluginSetting pluginSetting) {
        this.pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);
        recordsInCounter = pluginMetrics.counter(MetricNames.RECORDS_IN);
        timeElapsedTimer = pluginMetrics.timer(MetricNames.TIME_ELAPSED);
        Integer numRetries = (Integer) pluginSetting.getAttributeFromSettings(NUM_RETRIES);
        if (numRetries == null) {
            numRetries = NUM_DEFAULT_RETRIES;
        }
        retryThread = new Thread(new SinkThread(this, numRetries));
        if (!isReady()) {
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
        retryThread.stop();
    }

    public Thread.State getRetryThreadState() {
        return retryThread.getState();
    }
}
