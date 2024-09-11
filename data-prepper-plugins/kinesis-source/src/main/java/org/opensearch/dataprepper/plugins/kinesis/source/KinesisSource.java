/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source;

import lombok.Setter;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.extension.KinesisLeaseConfigSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DataPrepperPlugin(name = "kinesis", pluginType = Source.class, pluginConfigurationType = KinesisSourceConfig.class)
public class KinesisSource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);
    private final KinesisSourceConfig kinesisSourceConfig;
    private final KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier;

    @Setter
    private KinesisService kinesisService;

    @DataPrepperPluginConstructor
    public KinesisSource(final KinesisSourceConfig kinesisSourceConfig,
                         final PluginMetrics pluginMetrics,
                         final PluginFactory pluginFactory,
                         final PipelineDescription pipelineDescription,
                         final AwsCredentialsSupplier awsCredentialsSupplier,
                         final AcknowledgementSetManager acknowledgementSetManager,
                         final KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier) {
        this.kinesisSourceConfig = kinesisSourceConfig;
        this.kinesisLeaseConfigSupplier = kinesisLeaseConfigSupplier;
        KinesisClientFactory kinesisClientFactory = new KinesisClientFactory(awsCredentialsSupplier, kinesisSourceConfig.getAwsAuthenticationConfig());
        this.kinesisService = new KinesisService(kinesisSourceConfig, kinesisClientFactory, pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager, kinesisLeaseConfigSupplier, new HostNameWorkerIdentifierGenerator());
    }
    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        kinesisService.start(buffer);
    }

    @Override
    public void stop() {
        kinesisService.shutDown();
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return kinesisSourceConfig.isAcknowledgments();
    }
}
