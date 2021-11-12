/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.prepper;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.record.Record;
import java.util.Collection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

/**
 * Abstract implementation of the {@link Prepper} interface. This class implements an execute function which records
 * some basic metrics. Logic of the execute function is handled by extensions of this class in the doExecute function.
 */
public abstract class AbstractPrepper<InputRecord extends Record<?>, OutputRecord extends Record<?>> implements
        Prepper<InputRecord, OutputRecord> {

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
     * This function should implement the processing logic of the prepper
     * @param records Input records
     * @return Processed records
     */
    public abstract Collection<OutputRecord> doExecute(Collection<InputRecord> records);
}
