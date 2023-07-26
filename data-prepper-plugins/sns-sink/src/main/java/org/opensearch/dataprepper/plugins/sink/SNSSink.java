/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sns.SnsClient;

import java.util.Collection;

/**
 * Implementation class of sns-sink plugin. It is responsible for receive the collection of
 * {@link Event} and upload to amazon sns based on thresholds configured.
 */
@DataPrepperPlugin(name = "sns", pluginType = Sink.class, pluginConfigurationType = SNSSinkConfig.class)
public class SNSSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(SNSSink.class);

    private volatile boolean sinkInitialized;

    private final SNSSinkService snsSinkService;

    /**
     * @param pluginSetting dp plugin settings.
     * @param snsSinkConfig sns sink configurations.
     * @param pluginFactory dp plugin factory.
     */
    @DataPrepperPluginConstructor
    public SNSSink(final PluginSetting pluginSetting,
                  final SNSSinkConfig snsSinkConfig,
                  final PluginFactory pluginFactory,
                   final SinkContext sinkContext,
                  final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);
        final PluginModel codecConfiguration = snsSinkConfig.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());
        // TODO: Sink codec changes are pending
        // codec = pluginFactory.loadPlugin(Codec.class, codecPluginSettings);
        sinkInitialized = Boolean.FALSE;

        final SnsClient snsClient = SNSClientFactory.createSNSClient(snsSinkConfig, awsCredentialsSupplier);
        snsSinkService = new SNSSinkService(snsSinkConfig,snsClient,pluginMetrics,pluginFactory,pluginSetting);
    }

    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

    @Override
    public void doInitialize() {
        try {
            doInitializeInternal();
        } catch (InvalidPluginConfigurationException e) {
            LOG.error("Invalid plugin configuration, Hence failed to initialize sns-sink plugin.");
            this.shutdown();
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to initialize sns-sink plugin.");
            this.shutdown();
            throw e;
        }
    }

    /**
     * Initialize {@link SNSSinkService}
     */
    private void doInitializeInternal() {
        sinkInitialized = Boolean.TRUE;
    }

    /**
     * @param records Records to be output
     */
    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }
        snsSinkService.output(records);
    }
}