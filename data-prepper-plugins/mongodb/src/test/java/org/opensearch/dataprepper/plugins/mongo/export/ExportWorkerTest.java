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
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExportWorkerTest {
    private final String S3_PATH_PREFIX = UUID.randomUUID().toString();

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
    private RecordBufferWriter recordBufferWriter;

    private ExportWorker exportWorker;

    @BeforeEach
    public void setup() throws Exception {
        exportWorker = new ExportWorker(sourceCoordinator, buffer, pluginMetrics, acknowledgementSetManager, sourceConfig, S3_PATH_PREFIX);
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

    @Test
    void test_export_withNullS3PathPrefix() {
        assertThrows(IllegalArgumentException.class, () -> new ExportWorker(sourceCoordinator, buffer, pluginMetrics, acknowledgementSetManager, sourceConfig, null));
    }

}
