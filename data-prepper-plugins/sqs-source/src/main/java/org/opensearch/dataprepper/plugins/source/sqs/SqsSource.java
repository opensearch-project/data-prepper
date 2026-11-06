/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import java.util.Objects;

@DataPrepperPlugin(name = "sqs", pluginType = Source.class,pluginConfigurationType = SqsSourceConfig.class)
public class SqsSource implements Source<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(SqsSource.class);

    private final PluginMetrics pluginMetrics;
    private final SqsSourceConfig sqsSourceConfig;
    private SqsService sqsService;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final boolean acknowledgementsEnabled;


    @DataPrepperPluginConstructor
    public SqsSource(final PluginMetrics pluginMetrics,
                     final SqsSourceConfig sqsSourceConfig,
                     final AcknowledgementSetManager acknowledgementSetManager,
                     final AwsCredentialsSupplier awsCredentialsSupplier) {

        this.pluginMetrics = pluginMetrics;
        this.sqsSourceConfig = sqsSourceConfig;
        this.acknowledgementsEnabled = sqsSourceConfig.getAcknowledgements();
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.awsCredentialsSupplier = awsCredentialsSupplier;

        if (!acknowledgementsEnabled) {
            LOG.warn("SQS source acknowledgments are disabled. Messages will be deleted from SQS before " +
                    "delivery to the sink is confirmed. If the pipeline fails after receiving a message, " +
                    "that message will be permanently lost. Set 'acknowledgments: true' to enable " +
                    "end-to-end delivery confirmation and prevent data loss.");
        }
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer is null");
        }
        final AwsAuthenticationAdapter awsAuthenticationAdapter = new AwsAuthenticationAdapter(awsCredentialsSupplier, sqsSourceConfig);
        final AwsCredentialsProvider credentialsProvider = awsAuthenticationAdapter.getCredentialsProvider();
        sqsService = new SqsService(buffer, acknowledgementSetManager, sqsSourceConfig, pluginMetrics, credentialsProvider);
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
