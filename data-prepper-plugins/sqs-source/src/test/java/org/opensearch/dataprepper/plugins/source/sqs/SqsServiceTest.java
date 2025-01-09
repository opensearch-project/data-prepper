/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import java.util.List;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;


class SqsServiceTest {
    private SqsSourceConfig sqsSourceConfig;
    private SqsEventProcessor sqsEventProcessor;
    private SqsClient sqsClient;
    private PluginMetrics pluginMetrics;
    private AcknowledgementSetManager acknowledgementSetManager;
    private Buffer<Record<Event>> buffer;
    private AwsCredentialsProvider credentialsProvider;

    @BeforeEach
    void setUp() {
        sqsSourceConfig = mock(SqsSourceConfig.class);
        sqsEventProcessor = mock(SqsEventProcessor.class);
        sqsClient = mock(SqsClient.class, withSettings());
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
        QueueConfig queueConfig = mock(QueueConfig.class);
        when(queueConfig.getUrl()).thenReturn("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue");
        when(queueConfig.getNumWorkers()).thenReturn(2);
        when(sqsSourceConfig.getQueues()).thenReturn(List.of(queueConfig));
        SqsService sqsService = spy(new SqsService(buffer, acknowledgementSetManager, sqsSourceConfig, sqsEventProcessor, pluginMetrics, credentialsProvider));
        doReturn(sqsClient).when(sqsService).createSqsClient(credentialsProvider);
        sqsService.start(); // if no exception is thrown here, then workers have been started
    }

    @Test
    void stop_should_shutdown_executors_and_workers_and_close_client() throws InterruptedException {
        QueueConfig queueConfig = mock(QueueConfig.class);
        when(queueConfig.getUrl()).thenReturn("MyQueue");
        when(queueConfig.getNumWorkers()).thenReturn(1);
        when(sqsSourceConfig.getQueues()).thenReturn(List.of(queueConfig));
        SqsClient sqsClient = mock(SqsClient.class);
        SqsService sqsService = new SqsService(buffer, acknowledgementSetManager, sqsSourceConfig, sqsEventProcessor, pluginMetrics, credentialsProvider) {
            @Override
            SqsClient createSqsClient(final AwsCredentialsProvider credentialsProvider) {
                return sqsClient;
            }
        };
        sqsService.start();
        sqsService.stop();
        verify(sqsClient, times(1)).close();
    }

}