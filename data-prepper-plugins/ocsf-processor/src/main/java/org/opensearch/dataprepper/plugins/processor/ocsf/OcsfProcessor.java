/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.ocsf;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import java.util.Collection;


@DataPrepperPlugin(name = "ocsf", pluginType = Processor.class, pluginConfigurationType = OcsfProcessorConfig.class)
public class OcsfProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    OcsfTransformer ocsfTransformer;
    private static final String TRANSFORMATION_ERRORS = "transformationErrors";
    private final Counter transformationErrorsCounter;
    private final String version;

    @DataPrepperPluginConstructor
    public OcsfProcessor(final OcsfProcessorConfig ocsfProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        super(pluginMetrics);
        ocsfTransformer = loadOcsfTransformer(pluginFactory, ocsfProcessorConfig.getSchemaType());
        version = ocsfProcessorConfig.getVersion();
        transformationErrorsCounter = pluginMetrics.counter(TRANSFORMATION_ERRORS);
    }

    private OcsfTransformer loadOcsfTransformer(final PluginFactory pluginFactory, final PluginModel modeConfiguration) {
        final PluginSetting modePluginSetting = new PluginSetting(modeConfiguration.getPluginName(), modeConfiguration.getPluginSettings());
        return pluginFactory.loadPlugin(OcsfTransformer.class, modePluginSetting);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        for (final Record<Event> record: records) {
            try {
                ocsfTransformer.transform(record.getData(), version);
            } catch (Exception e) {
                transformationErrorsCounter.increment();
            }
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
}
