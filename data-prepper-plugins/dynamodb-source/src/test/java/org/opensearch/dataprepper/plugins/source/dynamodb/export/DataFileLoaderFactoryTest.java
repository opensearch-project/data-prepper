/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.DataFileProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@ExtendWith(MockitoExtension.class)
class DataFileLoaderFactoryTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private S3Client s3Client;
    @Mock
    private PluginMetrics pluginMetrics;


    private DataFilePartition dataFilePartition;

    @Mock
    private Buffer<Record<Event>> buffer;

    private TableInfo tableInfo;

    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String partitionKeyAttrName = "PK";
    private final String sortKeyAttrName = "SK";

    private final String manifestKey = UUID.randomUUID().toString();
    private final String bucketName = UUID.randomUUID().toString();
    private final String prefix = UUID.randomUUID().toString();

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";

    private final Random random = new Random();

    private final int total = random.nextInt(10);


    @BeforeEach
    void setup() {
        DataFileProgressState state = new DataFileProgressState();
        state.setLoaded(0);
        state.setTotal(total);
        dataFilePartition = new DataFilePartition(exportArn, bucketName, manifestKey, Optional.of(state));

        // Mock Global Table Info
        TableMetadata metadata = TableMetadata.builder()
                .exportRequired(true)
                .streamRequired(true)
                .partitionKeyAttributeName(partitionKeyAttrName)
                .sortKeyAttributeName(sortKeyAttrName)
                .build();

        tableInfo = new TableInfo(tableArn, metadata);
    }

    @Test
    void test_createDataFileLoader() {

        DataFileLoaderFactory loaderFactory = new DataFileLoaderFactory(coordinator, s3Client, pluginMetrics, buffer);
        Runnable loader = loaderFactory.createDataFileLoader(dataFilePartition, tableInfo);
        assertThat(loader, notNullValue());

    }
}