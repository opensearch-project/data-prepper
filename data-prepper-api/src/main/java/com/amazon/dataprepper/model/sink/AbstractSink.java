/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.model.sink;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.record.Record;
import java.util.Collection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

/**
 * This class implements the Sink interface and records boilerplate metrics
 */
public abstract class AbstractSink<T extends Record<?>> implements Sink<T> {
    protected final PluginMetrics pluginMetrics;
    private final Counter recordsInCounter;
    private final Timer timeElapsedTimer;

    public AbstractSink(final PluginSetting pluginSetting) {
        this.pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);
        recordsInCounter = pluginMetrics.counter(MetricNames.RECORDS_IN);
        timeElapsedTimer = pluginMetrics.timer(MetricNames.TIME_ELAPSED);
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
}
