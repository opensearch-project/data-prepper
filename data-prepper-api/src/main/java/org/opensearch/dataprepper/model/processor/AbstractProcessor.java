/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * @since 1.2
 * Abstract implementation of the {@link Processor} interface. This class implements an execute function which records
 * some basic metrics. Logic of the execute function is handled by extensions of this class in the doExecute function.
 */
public abstract class AbstractProcessor<InputRecord extends Record<?>, OutputRecord extends Record<?>> implements
        Processor<InputRecord, OutputRecord> {

    protected final PluginMetrics pluginMetrics;
    private final Counter recordsInCounter;
    private final Counter recordsOutCounter;
    private final Timer timeElapsedTimer;

    public AbstractProcessor(final PluginSetting pluginSetting) {
        pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);
        recordsInCounter = pluginMetrics.counter(MetricNames.RECORDS_IN);
        recordsOutCounter = pluginMetrics.counter(MetricNames.RECORDS_OUT);
        timeElapsedTimer = pluginMetrics.timer(MetricNames.TIME_ELAPSED);
    }

    protected AbstractProcessor(final PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
        recordsInCounter = pluginMetrics.counter(MetricNames.RECORDS_IN);
        recordsOutCounter = pluginMetrics.counter(MetricNames.RECORDS_OUT);
        timeElapsedTimer = pluginMetrics.timer(MetricNames.TIME_ELAPSED);
    }

    /**
     * @since 1.2
     * This execute function calls the {@link AbstractProcessor#doExecute(Collection)} function of the implementation,
     * and records metrics for records in, records out, and elapsed time.
     * @param records Input records that will be modified/processed
     * @return Records as processed by the doExecute function
     */
    @Override
    public Collection<OutputRecord> execute(final Collection<InputRecord> records) {
        recordsInCounter.increment(records.size());
        final Collection<OutputRecord> result = timeElapsedTimer.record(() -> doExecute(records));
        recordsOutCounter.increment(result.size());
        return result;
    }

    /**
     * @since 1.2
     * This function should implement the processing logic of the processor
     * @param records Input records
     * @return Processed records
     */
    public abstract Collection<OutputRecord> doExecute(Collection<InputRecord> records);
}
