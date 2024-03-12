/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DynamoDBServiceTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private ClientFactory clientFactory;

    @Mock
    private DynamoDbClient dynamoDbClient;

    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;

    @Mock
    private S3Client s3Client;

    @Mock
    private DynamoDBSourceConfig sourceConfig;

    @Mock
    private TableConfig tableConfig;

    @Mock
    private StreamConfig streamConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private ExecutorService executorService;

    private DynamoDBService dynamoDBService;

    @BeforeEach
    void setup() {
        // Mock Client Factory
        lenient().when(clientFactory.buildS3Client()).thenReturn(s3Client);
        lenient().when(clientFactory.buildDynamoDBClient()).thenReturn(dynamoDbClient);
        lenient().when(clientFactory.buildDynamoDbStreamClient()).thenReturn(dynamoDbStreamsClient);
        lenient().when(sourceConfig.getTableConfigs()).thenReturn(List.of(tableConfig));

    }

    private DynamoDBService createObjectUnderTest() {

        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(eq(4))).thenReturn(executorService);

            return new DynamoDBService(coordinator, clientFactory, sourceConfig, pluginMetrics, acknowledgementSetManager);
        }
    }

    @Test
    void test_normal_start_with_stream_config() {
        when(tableConfig.getStreamConfig()).thenReturn(streamConfig);
        dynamoDBService = createObjectUnderTest();
        assertThat(dynamoDBService, notNullValue());

        final ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);

        dynamoDBService.start(buffer);

        verify(executorService, times(4)).submit(runnableArgumentCaptor.capture());

        assertThat(runnableArgumentCaptor.getAllValues(), notNullValue());
        assertThat(runnableArgumentCaptor.getAllValues().size(), equalTo(4));
    }

    @Test
    void test_normal_start_without_stream_config() {
        when(tableConfig.getStreamConfig()).thenReturn(null);

        dynamoDBService = createObjectUnderTest();
        assertThat(dynamoDBService, notNullValue());

        final ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);

        dynamoDBService.start(buffer);

        verify(executorService, times(3)).submit(runnableArgumentCaptor.capture());

        assertThat(runnableArgumentCaptor.getAllValues(), notNullValue());
        assertThat(runnableArgumentCaptor.getAllValues().size(), equalTo(3));
    }


    @Test
    void test_normal_shutdown() {
        dynamoDBService = createObjectUnderTest();
        assertThat(dynamoDBService, notNullValue());

        when(executorService.shutdownNow()).thenReturn(Collections.emptyList());
        dynamoDBService.shutdown();
    }

}