/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB.MongoDBService;
import org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB.MongoDBSnapshotProgressState;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MongoDBSourceTest {
    @Mock
    private MongoDBConfig mongoDBConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private SourceCoordinator sourceCoordinator;

    @Mock
    private MongoDBService mongoDBService;

    @Mock
    private Buffer<Record<Object>> buffer;

    @BeforeEach
    void setup() {
        mongoDBConfig = mock(MongoDBConfig.class);
        sourceCoordinator = mock(SourceCoordinator.class);
    }

    @Test
    void testConstructorValidations() {
        when(mongoDBConfig.getIngestionMode()).thenReturn(MongoDBConfig.IngestionMode.EXPORT_STREAM);
        assertThrows(IllegalArgumentException.class, () -> new MongoDBSource(
                mongoDBConfig,
                pluginMetrics,
                pipelineDescription,
                acknowledgementSetManager,
                awsCredentialsSupplier,
                null,
                null));
    }

    @Test
    void testExportConstructor() {
        when(mongoDBConfig.getIngestionMode()).thenReturn(MongoDBConfig.IngestionMode.EXPORT);
        doNothing().when(sourceCoordinator).giveUpPartitions();
        MongoDBSource mongoDBSource = new MongoDBSource(
                mongoDBConfig,
                pluginMetrics,
                pipelineDescription,
                acknowledgementSetManager,
                awsCredentialsSupplier,
                null,
                null);
        mongoDBSource.setSourceCoordinator(sourceCoordinator);
        assertThat(mongoDBSource.getPartitionProgressStateClass(), equalTo(MongoDBSnapshotProgressState.class));
        assertThat(mongoDBSource.getDecoder(), instanceOf(ByteDecoder.class));
        try (MockedStatic<MongoDBService> mockedStatic = mockStatic((MongoDBService.class))) {
            mongoDBService = mock(MongoDBService.class);
            doNothing().when(mongoDBService).start();
            doNothing().when(mongoDBService).stop();
            mockedStatic.when(() -> MongoDBService.create(any(), any(), any(), any(), any())).thenReturn(mongoDBService);
            mongoDBSource.start(buffer);
            verify(mongoDBService).start();
            mongoDBSource.stop();
            verify(mongoDBService).stop();
            verify(sourceCoordinator).giveUpPartitions();
        }
    }
}
