/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;

@DataPrepperPlugin(name = "delay", pluginType = Processor.class, pluginConfigurationType = DelayProcessor.Configuration.class)
public class DelayProcessor implements Processor<Record<?>, Record<?>> {
    private static final Logger LOG = LoggerFactory.getLogger(DelayProcessor.class);
    private final Duration delayDuration;

    @DataPrepperPluginConstructor
    public DelayProcessor(final Configuration configuration) {
        delayDuration = configuration.getDelayFor();
        LOG.info("Delay processor configured for {}. The pipeline will delay for this time. This is typically only for testing or debugging.", delayDuration);
    }

    @Override
    public Collection<Record<?>> execute(final Collection<Record<?>> records) {
        try {
            Thread.sleep(delayDuration.toMillis());
        } catch (final InterruptedException ex) {
            LOG.error(NOISY, "Interrupted during delay processor", ex);
        }
        return records;
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {

    }

    @JsonPropertyOrder
    @JsonClassDescription("This processor will add a delay into the processor chain. " +
            "Typically, you should use this only for testing, experimenting, and debugging.")
    public static class Configuration {
        @JsonProperty("for")
        @JsonPropertyDescription("The duration of time to delay. Defaults to <code>1s</code>.")
        private Duration delayFor = Duration.ofSeconds(1);

        public Duration getDelayFor() {
            return delayFor;
        }
    }
}
