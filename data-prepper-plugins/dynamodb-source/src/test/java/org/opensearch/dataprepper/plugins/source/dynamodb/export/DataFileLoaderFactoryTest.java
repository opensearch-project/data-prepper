/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.ProgressCheck;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.DataFileProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.dataprepper.plugins.source.dynamodb.export.DataFileLoaderFactory.ACKNOWLEDGMENT_EXPIRY_INCREASE_TIME;

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
    private final String exportTime = "1976-01-01T00:00:00Z";

    private final Random random = new Random();

    private final int total = random.nextInt(10);


    @BeforeEach
    void setup() {
        DataFileProgressState state = new DataFileProgressState();
        state.setLoaded(0);
        state.setTotal(total);
        state.setStartTime(Instant.parse(exportTime).toEpochMilli());
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
        Runnable loader = loaderFactory.createDataFileLoader(dataFilePartition, tableInfo, null, null);
        assertThat(loader, notNullValue());
    }

    @Test
    void test_createDataFileLoader_with_acknowledgments() {
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        final Duration acknowledgmentTimeout = Duration.ofSeconds(30);

        final ArgumentCaptor<Consumer> progressCheckConsumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        doNothing().when(acknowledgementSet).addProgressCheck(any(Consumer.class), any(Duration.class));

        DataFileLoaderFactory loaderFactory = new DataFileLoaderFactory(coordinator, s3Client, pluginMetrics, buffer);

        Runnable loader = loaderFactory.createDataFileLoader(dataFilePartition, tableInfo, acknowledgementSet, acknowledgmentTimeout);
        assertThat(loader, notNullValue());

        verify(acknowledgementSet).addProgressCheck(progressCheckConsumerArgumentCaptor.capture(), any(Duration.class));

        final Consumer<ProgressCheck> progressCheckConsumer = progressCheckConsumerArgumentCaptor.getValue();


        progressCheckConsumer.accept(mock(ProgressCheck.class));

        verify(acknowledgementSet).increaseExpiry(eq(ACKNOWLEDGMENT_EXPIRY_INCREASE_TIME));
    }
}