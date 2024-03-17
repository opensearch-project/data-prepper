package org.opensearch.dataprepper.plugins.mongo.export;

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
import org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExportWorkerTest {
    private static final String TEST_COLLECTION_NAME = "test.collection";
    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private MongoDBSourceConfig sourceConfig;

    @Mock
    private CollectionConfig collectionConfig;

    @Mock
    private ExportRecordBufferWriter recordBufferWriter;

    private ExportWorker exportWorker;

    @BeforeEach
    public void setup() throws Exception {
        lenient().when(collectionConfig.getCollection()).thenReturn(TEST_COLLECTION_NAME);
        when(sourceConfig.getCollections()).thenReturn(List.of(collectionConfig));
        lenient().when(buffer.isByteBuffer()).thenReturn(false);
        lenient().doNothing().when(buffer).write(any(), anyInt());
        exportWorker = new ExportWorker(sourceCoordinator, buffer, pluginMetrics, acknowledgementSetManager, sourceConfig);
    }

    @Test
    void testEmptyDataQueryPartition() throws Exception {
        when(sourceCoordinator.acquireAvailablePartition(DataQueryPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> exportWorker.run());
        Thread.sleep(100);
        executorService.shutdownNow();

        verifyNoInteractions(recordBufferWriter);
    }

}
