package com.amazon.situp.model.processor;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.metrics.MetricNames;
import com.amazon.situp.model.record.Record;
import java.util.Collection;
import java.util.StringJoiner;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

public abstract class AbstractProcessor<InputRecord extends Record<?>, OutputRecord extends Record<?>> implements
    Processor<InputRecord, OutputRecord> {

    private final Counter recordsInCounter;
    private final Counter recordsOutCounter;
    private final Timer timeElapsedTimer;

    public AbstractProcessor(final PluginSetting pluginSetting) {
        final String qualifiedName = new StringJoiner(MetricNames.DELIMITER).add(pluginSetting.getPipelineName()).add(pluginSetting.getName()).toString();
        recordsInCounter = Metrics.counter(new StringJoiner(MetricNames.DELIMITER).add(qualifiedName).add(MetricNames.RECORDS_IN).toString());
        recordsOutCounter = Metrics.counter(new StringJoiner(MetricNames.DELIMITER).add(qualifiedName).add(MetricNames.RECORDS_OUT).toString());
        timeElapsedTimer = Metrics.timer(new StringJoiner(MetricNames.DELIMITER).add(qualifiedName).add(MetricNames.TIME_ELAPSED).toString());
    }

    /**
     * This execute function calls the {@link AbstractProcessor#doExecute(Collection)} function of the implementation,
     * and records metrics for records in, records out, and elapsed time.
     * @param records Input records that will be modified/processed
     * @return Records as processed by the doExecute function
     */
    @Override
    public Collection<OutputRecord> execute(Collection<InputRecord> records) {
        recordsInCounter.increment(records.size());
        try {
            final Collection<OutputRecord> result = timeElapsedTimer.recordCallable(() -> doExecute(records));
            recordsOutCounter.increment(result.size());
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This function should implement the processing logic of the processor
     * @param records Input records
     * @return Processed records
     */
    abstract Collection<OutputRecord> doExecute(Collection<InputRecord> records);
}
