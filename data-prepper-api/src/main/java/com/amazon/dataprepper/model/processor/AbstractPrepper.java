package com.amazon.situp.model.processor;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.metrics.MetricNames;
import com.amazon.situp.metrics.PluginMetrics;
import com.amazon.situp.model.record.Record;
import java.util.Collection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

/**
 * Abstract implementation of the {@link com.amazon.situp.model.processor.Processor} interface. This class implements an execute function which records
 * some basic metrics. Logic of the execute function is handled by extensions of this class in the doExecute function.
 */
public abstract class AbstractPrepper<InputRecord extends Record<?>, OutputRecord extends Record<?>> implements
    Processor<InputRecord, OutputRecord> {

    protected final PluginMetrics pluginMetrics;
    private final Counter recordsInCounter;
    private final Counter recordsOutCounter;
    private final Timer timeElapsedTimer;

    public AbstractPrepper(final PluginSetting pluginSetting) {
        pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);
        recordsInCounter = pluginMetrics.counter(MetricNames.RECORDS_IN);
        recordsOutCounter = pluginMetrics.counter(MetricNames.RECORDS_OUT);
        timeElapsedTimer = pluginMetrics.timer(MetricNames.TIME_ELAPSED);
    }

    /**
     * This execute function calls the {@link AbstractPrepper#doExecute(Collection)} function of the implementation,
     * and records metrics for records in, records out, and elapsed time.
     * @param records Input records that will be modified/processed
     * @return Records as processed by the doExecute function
     */
    @Override
    public Collection<OutputRecord> execute(Collection<InputRecord> records) {
        recordsInCounter.increment(records.size());
        final Collection<OutputRecord> result = timeElapsedTimer.record(() -> doExecute(records));
        recordsOutCounter.increment(result.size());
        return result;
    }

    /**
     * This function should implement the processing logic of the processor
     * @param records Input records
     * @return Processed records
     */
    abstract Collection<OutputRecord> doExecute(Collection<InputRecord> records);
}
