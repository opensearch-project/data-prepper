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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.awaitility.Awaitility.await;
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
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportPartitionWorker.FAILURE_ITEM_COUNTER_NAME;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportPartitionWorker.SUCCESS_ITEM_COUNTER_NAME;

@ExtendWith(MockitoExtension.class)
public class ExportPartitionWorkerTest {
    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;
    @Mock
    private DataQueryPartition dataQueryPartition;
    @Mock
    private RecordBufferWriter mockRecordBufferWriter;
    @Mock
    private AcknowledgementSet mockAcknowledgementSet;
    @Mock
    private MongoDBSourceConfig mockSourceConfig;
    @Mock
    private DataQueryPartitionCheckpoint mockPartitionCheckpoint;
    @Mock
    private PluginMetrics mockPluginMetrics;
    @Mock
    private Counter successItemsCounter;

    @Mock
    private Counter failureItemsCounter;
    private ExportPartitionWorker exportPartitionWorker;
    @BeforeEach
    public void setup() {
        when(mockPluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME)).thenReturn(successItemsCounter);
        when(mockPluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME)).thenReturn(failureItemsCounter);
        exportPartitionWorker = new ExportPartitionWorker(mockRecordBufferWriter, dataQueryPartition,
                mockAcknowledgementSet, mockSourceConfig, mockPartitionCheckpoint, mockPluginMetrics);
    }

    @ParameterizedTest
    @CsvSource({
            "test.collection|0|1|java.lang.Integer",
            "test.collection|0|1|java.lang.Double",
            "test.collection|0|1|java.lang.String",
            "test.collection|0|1|java.lang.Long",
            "test.collection|000000000000000000000000|000000000000000000000001|org.bson.types.ObjectId"
    })
    public void testProcessPartitionSuccess(final String partitionKey) {
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
        lenient().when(sourceCoordinator.acquireAvailablePartition(DataQueryPartition.PARTITION_TYPE))
                .thenReturn(Optional.of(dataQueryPartition));

        final Future<?> future = executorService.submit(() -> {
            try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
                mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                        .thenReturn(mongoClient);
                exportPartitionWorker.run();
            }
        });

        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() ->  verify(mongoClient).getDatabase(eq("test")));

        future.cancel(true);

        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() ->  verify(mockPartitionCheckpoint, times(2)).checkpoint(2));

        verify(mongoClient, times(1)).close();
        verify(mongoDatabase).getCollection(eq("collection"));
        verify(mockRecordBufferWriter).writeToBuffer(eq(mockAcknowledgementSet), any());
        verify(successItemsCounter, times(2)).increment();
        verify(failureItemsCounter, never()).increment();
    }
}