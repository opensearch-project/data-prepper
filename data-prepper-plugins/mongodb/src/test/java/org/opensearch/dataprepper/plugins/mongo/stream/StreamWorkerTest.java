package org.opensearch.dataprepper.plugins.mongo.stream;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.converter.PartitionKeyRecordConverter;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.mongo.model.S3PartitionStatus;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamWorker.BYTES_RECEIVED;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamWorker.FAILURE_ITEM_COUNTER_NAME;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamWorker.SUCCESS_ITEM_COUNTER_NAME;

@ExtendWith(MockitoExtension.class)
public class StreamWorkerTest {
    @Mock
    private RecordBufferWriter mockRecordBufferWriter;
    @Mock
    private PartitionKeyRecordConverter mockRecordConverter;
    @Mock
    private StreamAcknowledgementManager mockStreamAcknowledgementManager;
    @Mock
    private MongoDBSourceConfig mockSourceConfig;
    @Mock
    private DataStreamPartitionCheckpoint mockPartitionCheckpoint;
    @Mock
    private StreamPartition streamPartition;
    @Mock
    private StreamProgressState streamProgressState;
    @Mock
    private Counter successItemsCounter;
    @Mock
    private DistributionSummary bytesReceivedSummary;
    @Mock
    private Counter failureItemsCounter;

    @Mock
    private PluginMetrics mockPluginMetrics;

    private StreamWorker streamWorker;

    private final Random random = new Random();

    @BeforeEach
    public void setup() {
        when(mockPluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME)).thenReturn(successItemsCounter);
        when(mockPluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME)).thenReturn(failureItemsCounter);
        when(mockPluginMetrics.summary(BYTES_RECEIVED)).thenReturn(bytesReceivedSummary);
        when(mockSourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);
        //Thread.interrupted();
        streamWorker = new StreamWorker(mockRecordBufferWriter, mockRecordConverter, mockSourceConfig, mockStreamAcknowledgementManager,
                mockPartitionCheckpoint, mockPluginMetrics, 2, 0, 10_000, 1_000);
    }

    @Test
    void test_processStream_invalidCollection() {
        when(streamPartition.getCollection()).thenReturn(UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> streamWorker.processStream(streamPartition));
    }

    @Test
    void test_processStream_success() {
        final String collection = "database.collection";
        when(streamProgressState.shouldWaitForExport()).thenReturn(false);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        when(streamPartition.getCollection()).thenReturn(collection);
        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoCollection col = mock(MongoCollection.class);
        ChangeStreamIterable changeStreamIterable = mock(ChangeStreamIterable.class);
        MongoCursor cursor = mock(MongoCursor.class);
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(col);
        when(col.watch()).thenReturn(changeStreamIterable);
        when(changeStreamIterable.batchSize(1000)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, true, false);
        ChangeStreamDocument streamDoc1 = mock(ChangeStreamDocument.class);
        ChangeStreamDocument streamDoc2 = mock(ChangeStreamDocument.class);
        Document doc1 = mock(Document.class);
        Document doc2 = mock(Document.class);
        BsonDocument bsonDoc1 = new BsonDocument("resumeToken1", new BsonInt32(123));
        BsonDocument bsonDoc2 = new BsonDocument("resumeToken2", new BsonInt32(234));
        when(streamDoc1.getResumeToken()).thenReturn(bsonDoc1);
        when(streamDoc2.getResumeToken()).thenReturn(bsonDoc2);
        when(cursor.next())
            .thenReturn(streamDoc1)
            .thenReturn(streamDoc2);
        final String doc1Json1 = UUID.randomUUID().toString();
        final String doc1Json2 = UUID.randomUUID().toString();
        when(doc1.toJson(any(JsonWriterSettings.class))).thenReturn(doc1Json1);
        when(doc2.toJson(any(JsonWriterSettings.class))).thenReturn(doc1Json2);
        when(streamDoc1.getFullDocument()).thenReturn(doc1);
        when(streamDoc2.getFullDocument()).thenReturn(doc2);
        final String operationType1 = UUID.randomUUID().toString();
        final String operationType2 = UUID.randomUUID().toString();
        when(streamDoc1.getOperationTypeString()).thenReturn(operationType1);
        when(streamDoc2.getOperationTypeString()).thenReturn(operationType2);
        final BsonTimestamp bsonTimestamp1 = mock(BsonTimestamp.class);
        final BsonTimestamp bsonTimestamp2 = mock(BsonTimestamp.class);
        final int timeSecond1 = random.nextInt();
        final int timeSecond2 = random.nextInt();
        when(bsonTimestamp1.getTime()).thenReturn(timeSecond1);
        when(bsonTimestamp2.getTime()).thenReturn(timeSecond2);
        when(streamDoc1.getClusterTime()).thenReturn(bsonTimestamp1);
        when(streamDoc2.getClusterTime()).thenReturn(bsonTimestamp2);
        S3PartitionStatus s3PartitionStatus = mock(S3PartitionStatus.class);
        final List<String> partitions = List.of("first", "second");
        when(s3PartitionStatus.getPartitions()).thenReturn(partitions);
        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus()).thenReturn(Optional.of(s3PartitionStatus));

        try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
            mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                    .thenReturn(mongoClient);
            streamWorker.processStream(streamPartition);
        }
        verify(mongoClient).close();
        verify(mongoDatabase).getCollection(eq("collection"));
        verify(mockPartitionCheckpoint).getGlobalS3FolderCreationStatus();
        verify(mockRecordConverter).initializePartitions(partitions);
        verify(mockRecordConverter).convert(eq(doc1Json1), eq(timeSecond1 * 1000L), eq(timeSecond1 * 1000L), eq(operationType1));
        verify(mockRecordConverter).convert(eq(doc1Json2), eq(timeSecond2 * 1000L), eq(timeSecond2 * 1000L), eq(operationType2));
        verify(mockRecordBufferWriter).writeToBuffer(eq(null), any());
        verify(successItemsCounter).increment(2);
        verify(failureItemsCounter, never()).increment();
        verify(mockPartitionCheckpoint, times(2)).checkpoint("{\"resumeToken2\": 234}", 2);
    }


    @Test
    void test_processStream_mongoClientFailure() {
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        when(streamPartition.getCollection()).thenReturn("database.collection");
        MongoClient mongoClient = mock(MongoClient.class);
        try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
            mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                    .thenThrow(RuntimeException.class);
            assertThrows(RuntimeException.class, () -> streamWorker.processStream(streamPartition));
        }
        verifyNoInteractions(mongoClient);
        verifyNoInteractions(mockRecordBufferWriter);
        verifyNoInteractions(successItemsCounter);
        verifyNoInteractions(failureItemsCounter);
    }

    @Test
    void test_processStream_checkPointIntervalSuccess() {
        final String collection = "database.collection";
        when(streamProgressState.shouldWaitForExport()).thenReturn(false);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        when(streamPartition.getCollection()).thenReturn(collection);
        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoCollection col = mock(MongoCollection.class);
        ChangeStreamIterable changeStreamIterable = mock(ChangeStreamIterable.class);
        MongoCursor cursor = mock(MongoCursor.class);
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(col);
        when(col.watch()).thenReturn(changeStreamIterable);
        when(changeStreamIterable.batchSize(1000)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        ChangeStreamDocument streamDoc1 = mock(ChangeStreamDocument.class);
        ChangeStreamDocument streamDoc2 = mock(ChangeStreamDocument.class);
        ChangeStreamDocument streamDoc3 = mock(ChangeStreamDocument.class);
        Document doc1 = mock(Document.class);
        Document doc2 = mock(Document.class);
        Document doc3 = mock(Document.class);
        BsonDocument bsonDoc1 = mock(BsonDocument.class);
        BsonDocument bsonDoc2 = mock(BsonDocument.class);
        BsonDocument bsonDoc3 = mock(BsonDocument.class);
        when(streamDoc1.getResumeToken()).thenReturn(bsonDoc1);
        when(streamDoc2.getResumeToken()).thenReturn(bsonDoc2);
        when(streamDoc3.getResumeToken()).thenReturn(bsonDoc3);
        when(cursor.next())
            .thenReturn(streamDoc1, streamDoc2, streamDoc3);
        when(doc1.toJson(any(JsonWriterSettings.class))).thenReturn(UUID.randomUUID().toString());
        when(doc2.toJson(any(JsonWriterSettings.class))).thenReturn(UUID.randomUUID().toString());
        when(doc3.toJson(any(JsonWriterSettings.class))).thenReturn(UUID.randomUUID().toString());
        when(streamDoc1.getFullDocument()).thenReturn(doc1);
        when(streamDoc2.getFullDocument()).thenReturn(doc2);
        when(streamDoc3.getFullDocument()).thenReturn(doc3);
        final BsonTimestamp bsonTimestamp1 = mock(BsonTimestamp.class);
        final BsonTimestamp bsonTimestamp2 = mock(BsonTimestamp.class);
        final BsonTimestamp bsonTimestamp3 = mock(BsonTimestamp.class);
        final int timeSecond1 = random.nextInt();
        final int timeSecond2 = random.nextInt();
        final int timeSecond3 = random.nextInt();
        when(bsonTimestamp1.getTime()).thenReturn(timeSecond1);
        when(bsonTimestamp2.getTime()).thenReturn(timeSecond2);
        when(bsonTimestamp3.getTime()).thenReturn(timeSecond3);
        when(streamDoc1.getClusterTime()).thenReturn(bsonTimestamp1);
        when(streamDoc2.getClusterTime()).thenReturn(bsonTimestamp2);
        when(streamDoc3.getClusterTime()).thenReturn(bsonTimestamp3);
        final String resumeToken1 = UUID.randomUUID().toString();
        final String resumeToken2 = UUID.randomUUID().toString();
        final String resumeToken3 = UUID.randomUUID().toString();
        when(bsonDoc1.toJson(any(JsonWriterSettings.class))).thenReturn(resumeToken1);
        when(bsonDoc2.toJson(any(JsonWriterSettings.class))).thenReturn(resumeToken2);
        when(bsonDoc3.toJson(any(JsonWriterSettings.class))).thenReturn(resumeToken3);
        S3PartitionStatus s3PartitionStatus = mock(S3PartitionStatus.class);
        final List<String> partitions = List.of("first", "second");
        when(s3PartitionStatus.getPartitions()).thenReturn(partitions);
        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus()).thenReturn(Optional.of(s3PartitionStatus));
        try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {

            mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                    .thenReturn(mongoClient);
            streamWorker.processStream(streamPartition);

        }
        verify(mongoClient, times(1)).close();
        verify(mongoDatabase).getCollection(eq("collection"));
        verify(cursor).close();
        verify(cursor, times(4)).hasNext();
        verify(mockPartitionCheckpoint).getGlobalS3FolderCreationStatus();
        verify(mockPartitionCheckpoint).checkpoint(resumeToken3, 3);
        verify(successItemsCounter).increment(1);
        verify(mockPartitionCheckpoint).checkpoint(resumeToken2, 2);
        verify(mockRecordBufferWriter, times(2)).writeToBuffer(eq(null), any());
        verify(successItemsCounter).increment(2);
        verify(failureItemsCounter, never()).increment();
    }

    @Test
    void test_processStream_stopWorker() {
        final String collection = "database.collection";
        when(streamProgressState.shouldWaitForExport()).thenReturn(false);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        when(streamPartition.getCollection()).thenReturn(collection);
        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoCollection col = mock(MongoCollection.class);
        ChangeStreamIterable changeStreamIterable = mock(ChangeStreamIterable.class);
        MongoCursor cursor = mock(MongoCursor.class);
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString())).thenReturn(col);
        when(col.watch()).thenReturn(changeStreamIterable);
        when(changeStreamIterable.batchSize(1000)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.iterator()).thenReturn(cursor);
        S3PartitionStatus s3PartitionStatus = mock(S3PartitionStatus.class);
        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus()).thenReturn(Optional.of(s3PartitionStatus));
        final List<String> partitions = List.of("first", "second");
        when(s3PartitionStatus.getPartitions()).thenReturn(partitions);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> {
            try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
                mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                        .thenReturn(mongoClient);
                streamWorker.processStream(streamPartition);
            }
        });
        streamWorker.stop();
        await()
            .atMost(Duration.ofSeconds(4))
            .untilAsserted(() ->  verify(mongoClient).close());
        future.cancel(true);
        executorService.shutdownNow();
        verify(mongoDatabase).getCollection(eq("collection"));
    }
}
