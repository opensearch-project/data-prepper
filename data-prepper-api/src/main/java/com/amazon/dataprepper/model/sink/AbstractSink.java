package com.amazon.situp.model.sink;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.metrics.MetricNames;
import com.amazon.situp.metrics.PluginMetrics;
import com.amazon.situp.model.record.Record;
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
