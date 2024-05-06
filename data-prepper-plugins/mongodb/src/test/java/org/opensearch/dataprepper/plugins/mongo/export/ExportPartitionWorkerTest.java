package org.opensearch.dataprepper.plugins.mongo.export;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.converter.PartitionKeyRecordConverter;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.model.S3PartitionStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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
import static org.opensearch.dataprepper.plugins.mongo.export.ExportPartitionWorker.BYTES_RECEIVED;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportPartitionWorker.FAILURE_ITEM_COUNTER_NAME;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportPartitionWorker.SUCCESS_ITEM_COUNTER_NAME;
import static org.opensearch.dataprepper.plugins.mongo.export.ExportPartitionWorker.VERSION_OVERLAP_TIME_FOR_EXPORT;

@ExtendWith(MockitoExtension.class)
public class ExportPartitionWorkerTest {
    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;
    @Mock
    private DataQueryPartition dataQueryPartition;
    @Mock
    private RecordBufferWriter mockRecordBufferWriter;
    @Mock
    private PartitionKeyRecordConverter mockRecordConverter;
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
    private DistributionSummary bytesReceivedSummary;

    @Mock
    private Counter failureItemsCounter;
    private ExportPartitionWorker exportPartitionWorker;
    private long exportStartTime;
    @BeforeEach
    public void setup() {
        exportStartTime = Instant.now().toEpochMilli();
        when(mockPluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME)).thenReturn(successItemsCounter);
        when(mockPluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME)).thenReturn(failureItemsCounter);
        when(mockPluginMetrics.summary(BYTES_RECEIVED)).thenReturn(bytesReceivedSummary);
        exportPartitionWorker = new ExportPartitionWorker(mockRecordBufferWriter, mockRecordConverter, dataQueryPartition,
                mockAcknowledgementSet, mockSourceConfig, mockPartitionCheckpoint, exportStartTime, mockPluginMetrics);
    }

    @ParameterizedTest
    @CsvSource({
            "test.collection|0|1|java.lang.Integer|java.lang.Integer",
            "test.collection|0|abc|java.lang.Double|java.lang.String",
            "test.collection|0|1|java.lang.String|java.lang.String",
            "test.collection|0|000000000000000000000000|java.lang.Long|org.bson.types.ObjectId",
            "test.collection|000000000000000000000000|000000000000000000000001|org.bson.types.ObjectId|org.bson.types.ObjectId"
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
        Document doc1 = mock(Document.class);
        Document doc2 = mock(Document.class);
        final String docJson1 = UUID.randomUUID().toString();
        final String docJson2 = UUID.randomUUID() + docJson1;
        when(doc1.toJson(any(JsonWriterSettings.class))).thenReturn(docJson1);
        when(doc2.toJson(any(JsonWriterSettings.class))).thenReturn(docJson2);
        lenient().when(cursor.next())
                .thenReturn(doc1)
                .thenReturn(doc2);
        final long eventVersionNumber = (exportStartTime - VERSION_OVERLAP_TIME_FOR_EXPORT.toMillis()) * 1_000;
        Event event1 = mock((Event.class));
        Event event2 = mock((Event.class));
        when(mockRecordConverter.convert(docJson1, exportStartTime, eventVersionNumber)).thenReturn(event1);
        when(mockRecordConverter.convert(docJson2, exportStartTime, eventVersionNumber)).thenReturn(event2);
        lenient().when(dataQueryPartition.getPartitionKey()).thenReturn(partitionKey);
        lenient().when(sourceCoordinator.acquireAvailablePartition(DataQueryPartition.PARTITION_TYPE))
                .thenReturn(Optional.of(dataQueryPartition));
        final String collection = partitionKey.split("\\|")[0];
        when(dataQueryPartition.getCollection()).thenReturn(collection);

        S3PartitionStatus s3PartitionStatus = mock(S3PartitionStatus.class);
        final List<String> partitions = List.of("first", "second");
        when(s3PartitionStatus.getPartitions()).thenReturn(partitions);
        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus(collection)).thenReturn(Optional.of(s3PartitionStatus));

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

        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() ->  verify(mockPartitionCheckpoint, times(2)).checkpoint(2));

        future.cancel(true);

        verify(mongoClient, times(1)).close();
        verify(mockRecordConverter).convert(docJson1, exportStartTime, eventVersionNumber);
        verify(mockRecordConverter).convert(docJson2, exportStartTime, eventVersionNumber);
        verify(mongoDatabase).getCollection(eq("collection"));
        verify(mockRecordConverter).initializePartitions(partitions);
        verify(mockRecordBufferWriter).writeToBuffer(eq(mockAcknowledgementSet), any());
        verify(successItemsCounter, times(2)).increment();
        verify(bytesReceivedSummary).record(docJson1.getBytes().length);
        verify(bytesReceivedSummary).record(docJson2.getBytes().length);
        verify(failureItemsCounter, never()).increment();
    }
}