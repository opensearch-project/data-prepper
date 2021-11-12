/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.prepper;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.record.Record;
import java.util.Collection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

/**
 * @deprecated as of 1.2, replaced by {@link com.amazon.dataprepper.model.processor.AbstractProcessor}
 * Abstract implementation of the {@link Prepper} interface. This class implements an execute function which records
 * some basic metrics. Logic of the execute function is handled by extensions of this class in the doExecute function.
 */
@Deprecated
public abstract class AbstractPrepper<InputRecord extends Record<?>, OutputRecord extends Record<?>> extends
        AbstractProcessor<InputRecord, OutputRecord> {
    public AbstractPrepper(final PluginSetting pluginSetting) {
        super(pluginSetting);
    }
}
