/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import com.linecorp.armeria.client.retry.Backoff;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.aws.sqs.common.ClientFactory;
import org.opensearch.dataprepper.plugins.aws.sqs.common.SqsService;
import org.opensearch.dataprepper.plugins.aws.sqs.common.handler.SqsMessageHandler;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;
import org.opensearch.dataprepper.plugins.aws.sqs.common.model.SqsOptions;
import org.opensearch.dataprepper.plugins.source.sqssource.config.SqsSourceConfig;
import org.opensearch.dataprepper.plugins.source.sqssource.handler.RawSqsMessageHandler;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@DataPrepperPlugin(name = "sqs", pluginType = Source.class,pluginConfigurationType = SqsSourceConfig.class)
public class SqsSource implements Source<Record<Event>> {

    static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();

    static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();

    static final double JITTER_RATE = 0.20;

    private final SqsSourceConfig sqsSourceConfig;

    private final AcknowledgementSetManager acknowledgementSetManager;

    private final PluginMetrics pluginMetrics;

    private final boolean acknowledgementsEnabled;

    private final org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier awsCredentialsSupplier;

    private final List<ExecutorService> executorServices;

    @DataPrepperPluginConstructor
    public SqsSource(final PluginMetrics pluginMetrics,
                     final SqsSourceConfig sqsSourceConfig,
                     final AcknowledgementSetManager acknowledgementSetManager,
                     final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.sqsSourceConfig = sqsSourceConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.acknowledgementsEnabled = sqsSourceConfig.getAcknowledgements();
        this.pluginMetrics = pluginMetrics;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.executorServices = new ArrayList<>(sqsSourceConfig.getQueues().getUrls().size());
    }


    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer is null");
        }
        final SqsMetrics sqsMetrics = new SqsMetrics(pluginMetrics);

        SqsClient sqsClient = ClientFactory.createSqsClient(sqsSourceConfig.getAws().getAwsRegion(),
                sqsSourceConfig.getAws().getAwsStsRoleArn(),
                sqsSourceConfig.getAws().getAwsStsHeaderOverrides(),
                awsCredentialsSupplier);

        final Backoff backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
        SqsService sqsService = new SqsService(sqsMetrics,sqsClient,backoff);
        SqsMessageHandler sqsHandler = new RawSqsMessageHandler(buffer,sqsService);
        SqsOptions.Builder sqsOptionsBuilder = new SqsOptions.Builder()
                .setPollDelay(sqsSourceConfig.getQueues().getPollingFrequency())
                .setMaximumMessages(sqsSourceConfig.getQueues().getBatchSize());
        sqsSourceConfig.getQueues().getUrls().forEach(url -> {
                final ExecutorService executorService = Executors.newFixedThreadPool(sqsSourceConfig.getQueues().getNumberOfThreads());
                executorServices.add(executorService);
                executorService.submit(new SqsSourceTask(sqsService,
                        sqsOptionsBuilder.setSqsUrl(url).build(),
                        sqsMetrics,
                        acknowledgementSetManager,
                        sqsSourceConfig.getAcknowledgements(), sqsHandler));
        });
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }

    @Override
    public void stop() {
        executorServices.forEach(executor -> executor.shutdown());
    }
}
