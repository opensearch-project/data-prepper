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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@DataPrepperPlugin(name = "sqs", pluginType = Source.class,pluginConfigurationType = SqsSourceConfig.class)
public class SqsSource implements Source<Record<Event>> {

    static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();

    static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(10);

    static final int NO_OF_RECORDS_TO_ACCUMULATE = 100;

    static final double JITTER_RATE = 0.20;

    private final SqsSourceConfig sqsSourceConfig;

    private final AcknowledgementSetManager acknowledgementSetManager;

    private final PluginMetrics pluginMetrics;

    private final boolean acknowledgementsEnabled;

    private final AwsCredentialsSupplier awsCredentialsSupplier;

    private final ScheduledExecutorService scheduledExecutorService;

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
        this.scheduledExecutorService = Executors.newScheduledThreadPool(sqsSourceConfig.getNumberOfThreads());
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer is null");
        }

        final SqsMetrics sqsMetrics = new SqsMetrics(pluginMetrics);

        final SqsClient sqsClient = ClientFactory.createSqsClient(sqsSourceConfig.getAws().getAwsRegion(),
                sqsSourceConfig.getAws().getAwsStsRoleArn(),
                sqsSourceConfig.getAws().getAwsStsHeaderOverrides(),
                awsCredentialsSupplier);

        final Backoff backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
        final SqsService sqsService = new SqsService(sqsMetrics,sqsClient,backoff);

        final SqsMessageHandler sqsHandler = new RawSqsMessageHandler(sqsService);
        final SqsOptions.Builder sqsOptionsBuilder = new SqsOptions.Builder()
                .setPollDelay(sqsSourceConfig.getPollingFrequency())
                .setVisibilityTimeout(sqsSourceConfig.getVisibilityTimeout())
                .setWaitTime(sqsSourceConfig.getWaitTime())
                .setMaximumMessages(sqsSourceConfig.getBatchSize());
        final long pollingFrequencyInMillis = sqsSourceConfig.getPollingFrequency().toMillis();
        sqsSourceConfig.getUrls().forEach(url -> {
            scheduledExecutorService.scheduleAtFixedRate(new SqsSourceTask(buffer,NO_OF_RECORDS_TO_ACCUMULATE,BUFFER_TIMEOUT,
                    sqsService,
                    sqsOptionsBuilder.setSqsUrl(url).build(),
                    sqsMetrics,
                    acknowledgementSetManager,
                    sqsSourceConfig.getAcknowledgements(),
                    sqsHandler)
                    ,0, pollingFrequencyInMillis == 0 ? 1 : pollingFrequencyInMillis,
                    TimeUnit.MILLISECONDS);
        });
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }

    @Override
    public void stop() {
        scheduledExecutorService.shutdown();
    }
}
