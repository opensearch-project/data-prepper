package org.opensearch.dataprepper.plugins.mongo.export;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.micrometer.core.instrument.Counter;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportWorker.FAILURE_ITEM_COUNTER_NAME;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportWorker.FAILURE_PARTITION_COUNTER_NAME;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportWorker.SUCCESS_ITEM_COUNTER_NAME;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportWorker.SUCCESS_PARTITION_COUNTER_NAME;

@ExtendWith(MockitoExtension.class)
public class ExportWorkerTest {
    private static final String TEST_COLLECTION_NAME = "test.collection";
    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private DataQueryPartition dataQueryPartition;

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

    @Mock
    private Counter successItemsCounter;

    @Mock
    private Counter failureItemsCounter;

    @Mock
    private Counter successPartitionCounter;

    @Mock
    private Counter failureParitionCounter;

    private ExportWorker exportWorker;

    @BeforeEach
    public void setup() throws Exception {
        lenient().when(collectionConfig.getCollection()).thenReturn(TEST_COLLECTION_NAME);
        when(sourceConfig.getCollections()).thenReturn(List.of(collectionConfig));
        lenient().when(buffer.isByteBuffer()).thenReturn(false);
        lenient().doNothing().when(buffer).write(any(), anyInt());
        lenient().when(pluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME)).thenReturn(successItemsCounter);
        lenient().when(pluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME)).thenReturn(failureItemsCounter);
        lenient().when(pluginMetrics.counter(SUCCESS_PARTITION_COUNTER_NAME)).thenReturn(successPartitionCounter);
        lenient().when(pluginMetrics.counter(FAILURE_PARTITION_COUNTER_NAME)).thenReturn(failureParitionCounter);
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

    @ParameterizedTest
    @CsvSource({
            "test.collection|0|1|java.lang.Integer",
            "test.collection|0|1|java.lang.Double",
            "test.collection|0|1|java.lang.String",
            "test.collection|0|1|java.lang.Long",
            "test.collection|000000000000000000000000|000000000000000000000001|org.bson.types.ObjectId"
    })
    public void test_shouldProcessPartitionSuccess(final String partitionKey) throws Exception {
        when(sourceCoordinator.acquireAvailablePartition(DataQueryPartition.PARTITION_TYPE)).thenReturn(Optional.of(dataQueryPartition));

        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoCollection col = mock(MongoCollection.class);
        FindIterable findIterable = mock(FindIterable.class);
        MongoCursor cursor = mock(MongoCursor.class);
        lenient().when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        lenient().when(mongoDatabase.getCollection(anyString())).thenReturn(col);
        lenient().when(col.find()).thenReturn(findIterable);
        lenient().when(col.find(any(Bson.class))).thenReturn(findIterable);
        lenient().when(findIterable.projection(any())).thenReturn(findIterable);
        lenient().when(findIterable.sort(any())).thenReturn(findIterable);
        lenient().when(findIterable.skip(anyInt())).thenReturn(findIterable);
        lenient().when(findIterable.limit(anyInt())).thenReturn(findIterable);
        lenient().when(findIterable.iterator()).thenReturn(cursor);
        lenient().when(cursor.hasNext()).thenReturn(true, true, false);
        lenient().when(cursor.next())
                .thenReturn(new Document("_id", 0))
                .thenReturn(new Document("_id", 1));
        lenient().when(dataQueryPartition.getPartitionKey()).thenReturn(partitionKey);
        lenient().when(sourceCoordinator.acquireAvailablePartition(DataQueryPartition.PARTITION_TYPE)).thenReturn(Optional.of(dataQueryPartition));


        final Future<?> future = executorService.submit(() -> {
            try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
                mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                        .thenReturn(mongoClient);
                exportWorker.run();
            }
        });

        Thread.sleep(100);
        executorService.shutdownNow();
        // Then dependencies are called
        verify(mongoClient).getDatabase(eq("test"));
        verify(mongoClient, times(1)).close();
        verify(mongoDatabase).getCollection(eq("collection"));
        verify(successItemsCounter, times(2)).increment();
        verify(failureItemsCounter, never()).increment();
    }
}
