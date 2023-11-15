/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;

import java.util.Optional;

import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MongoDBServiceTest {
    @Mock
    private MongoDBConfig mongoDBConfig;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator;

    @Mock
    private MongoDBPartitionCreationSupplier mongoDBPartitionCreationSupplier;

    @Mock
    private PluginMetrics pluginMetrics;

    @Test
    public void testConstructor() {
        createObjectUnderTest();
        verify(sourceCoordinator).initialize();
    }

    @Test
    public void testStartAndStop() throws InterruptedException {
        when(sourceCoordinator.getNextPartition(mongoDBPartitionCreationSupplier)).thenReturn(Optional.empty());
        MongoDBService testObject = createObjectUnderTest();
        testObject.start();
        Thread.sleep(100);
        testObject.stop();
    }

    private MongoDBService createObjectUnderTest() {
        try (final MockedConstruction<MongoDBPartitionCreationSupplier> mockedConstruction = mockConstruction(MongoDBPartitionCreationSupplier.class, (mock, context) -> {
            mongoDBPartitionCreationSupplier = mock;
        })) {
            return MongoDBService.create(mongoDBConfig, sourceCoordinator, buffer, acknowledgementSetManager, pluginMetrics);
        }
    }
}
