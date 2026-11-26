/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import com.linecorp.armeria.client.retry.Backoff;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.sqs.common.SqsBackoff;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class SqsServiceTest {
    private SqsSourceConfig sqsSourceConfig;
    private PluginMetrics pluginMetrics;
    private AcknowledgementSetManager acknowledgementSetManager;
    private Buffer<Record<Event>> buffer;
    private AwsCredentialsProvider credentialsProvider;

    @BeforeEach
    void setUp() {
        sqsSourceConfig = mock(SqsSourceConfig.class);
        pluginMetrics = mock(PluginMetrics.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        buffer = mock(Buffer.class);
        credentialsProvider = mock(AwsCredentialsProvider.class);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(sqsSourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
    }

    @Test
    void start_with_single_queue_starts_workers() {
        final Duration configuredBackoff = Duration.ofMinutes(10);
        QueueConfig queueConfig = mock(QueueConfig.class);
        when(queueConfig.getUrl()).thenReturn("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue");
        when(queueConfig.getNumWorkers()).thenReturn(2);
        when(queueConfig.getMaximumBackoff()).thenReturn(configuredBackoff);
        when(sqsSourceConfig.getQueues()).thenReturn(List.of(queueConfig));

        try (final MockedStatic<SqsBackoff> sqsBackoffMockedStatic = mockStatic(SqsBackoff.class)) {
            sqsBackoffMockedStatic.when(() -> SqsBackoff.createExponentialBackoff(any(Duration.class)))
                    .thenReturn(mock(Backoff.class));

            SqsService sqsService = spy(new SqsService(buffer, acknowledgementSetManager, sqsSourceConfig, pluginMetrics, credentialsProvider));
            sqsService.start();

            sqsBackoffMockedStatic.verify(() -> SqsBackoff.createExponentialBackoff(configuredBackoff));
        }
    }

    @Test
    void stop_should_shutdown_executors_and_workers() throws InterruptedException {
        final Duration configuredBackoff = Duration.ofMinutes(3);
        QueueConfig queueConfig = mock(QueueConfig.class);
        when(queueConfig.getUrl()).thenReturn("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue");
        when(queueConfig.getNumWorkers()).thenReturn(1);
        when(queueConfig.getMaximumBackoff()).thenReturn(configuredBackoff);
        when(sqsSourceConfig.getQueues()).thenReturn(List.of(queueConfig));

        try (final MockedStatic<SqsBackoff> sqsBackoffMockedStatic = mockStatic(SqsBackoff.class)) {
            sqsBackoffMockedStatic.when(() -> SqsBackoff.createExponentialBackoff(any(Duration.class)))
                    .thenReturn(mock(Backoff.class));

            SqsService sqsService = new SqsService(buffer, acknowledgementSetManager, sqsSourceConfig, pluginMetrics, credentialsProvider);
            sqsService.start();
            sqsService.stop();

            sqsBackoffMockedStatic.verify(() -> SqsBackoff.createExponentialBackoff(configuredBackoff));
        }
    }

}