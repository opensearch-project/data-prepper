/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.TableConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.lenient;

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
    private PluginMetrics pluginMetrics;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

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
        DynamoDBService objectUnderTest = new DynamoDBService(coordinator, clientFactory, sourceConfig, pluginMetrics, acknowledgementSetManager);
        return objectUnderTest;
    }

    @Test
    void test_normal_start() {
        dynamoDBService = createObjectUnderTest();
        assertThat(dynamoDBService, notNullValue());
        dynamoDBService.start(buffer);

    }


    @Test
    void test_normal_shutdown() {
        dynamoDBService = createObjectUnderTest();
        assertThat(dynamoDBService, notNullValue());
        dynamoDBService.shutdown();
    }

}