/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import java.util.Objects;

@DataPrepperPlugin(name = "sqs", pluginType = Source.class,pluginConfigurationType = SqsSourceConfig.class)
public class SqsSource implements Source<Record<Event>> {

    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;
    private final SqsSourceConfig sqsSourceConfig;
    private SqsService sqsService; 
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final boolean acknowledgementsEnabled;


    @DataPrepperPluginConstructor
    public SqsSource(final PluginMetrics pluginMetrics,
                     final SqsSourceConfig sqsSourceConfig,
                     final PluginFactory pluginFactory,
                     final AcknowledgementSetManager acknowledgementSetManager,
                     final AwsCredentialsSupplier awsCredentialsSupplier) {
                        
        this.pluginMetrics = pluginMetrics;
        this.pluginFactory = pluginFactory;
        this.sqsSourceConfig = sqsSourceConfig;
        this.acknowledgementsEnabled = sqsSourceConfig.getAcknowledgements();
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.awsCredentialsSupplier = awsCredentialsSupplier;

    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer is null");
        }
        final AwsAuthenticationAdapter awsAuthenticationAdapter = new AwsAuthenticationAdapter(awsCredentialsSupplier, sqsSourceConfig);
        final AwsCredentialsProvider credentialsProvider = awsAuthenticationAdapter.getCredentialsProvider();
        sqsService = new SqsService(buffer, acknowledgementSetManager, sqsSourceConfig, pluginMetrics, pluginFactory, credentialsProvider);
        sqsService.start();
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }

    @Override
    public void stop() {
        if (Objects.nonNull(sqsService)) {
            sqsService.stop();
        }
    }
}
