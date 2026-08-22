/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import org.opensearch.dataprepper.http.BaseHttpService;
import org.opensearch.dataprepper.http.BaseHttpSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.LoggerFactory;

import java.util.Objects;

@DataPrepperPlugin(name = "splunk_hec", pluginType = Source.class, pluginConfigurationType = SplunkHecSourceConfig.class)
@Experimental
public class SplunkHecSource extends BaseHttpSource<Record<Event>> {

    private static final String SOURCE_NAME = "Splunk HEC";

    private final SplunkHecSourceConfig sourceConfig;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private volatile SplunkHecService splunkHecService;

    @DataPrepperPluginConstructor
    public SplunkHecSource(final SplunkHecSourceConfig sourceConfig,
                           final PluginMetrics pluginMetrics,
                           final PluginFactory pluginFactory,
                           final PipelineDescription pipelineDescription,
                           final AcknowledgementSetManager acknowledgementSetManager) {
        super(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription, SOURCE_NAME,
                LoggerFactory.getLogger(SplunkHecSource.class));
        this.sourceConfig = Objects.requireNonNull(sourceConfig, "sourceConfig must not be null");
        this.acknowledgementSetManager = Objects.requireNonNull(acknowledgementSetManager,
                "acknowledgementSetManager must not be null");
    }

    @Override
    public BaseHttpService getHttpService(final int bufferWriteTimeoutInMillis,
                                          final Buffer<Record<Event>> buffer,
                                          final PluginMetrics pluginMetrics) {
        splunkHecService = createSplunkHecService(bufferWriteTimeoutInMillis, buffer, pluginMetrics);
        return splunkHecService;
    }

    SplunkHecService createSplunkHecService(final int bufferWriteTimeoutInMillis,
                                            final Buffer<Record<Event>> buffer,
                                            final PluginMetrics pluginMetrics) {
        return new SplunkHecService(bufferWriteTimeoutInMillis, buffer, pluginMetrics,
                sourceConfig, acknowledgementSetManager);
    }

    @Override
    public void stop() {
        if (splunkHecService != null) {
            splunkHecService.shutdown();
        }
        super.stop();
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return sourceConfig.isAcknowledgements();
    }
}
