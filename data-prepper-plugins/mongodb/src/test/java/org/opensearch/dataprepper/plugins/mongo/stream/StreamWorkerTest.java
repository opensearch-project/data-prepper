package org.opensearch.dataprepper.plugins.mongo.stream;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
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
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.DOCUMENTDB_ID_FIELD_NAME;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamWorker.BYTES_PROCESSED;
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
    private DistributionSummary bytesProcessedSummary;
    @Mock
    private Counter failureItemsCounter;

    @Mock
    private PluginMetrics mockPluginMetrics;

    private StreamWorker streamWorker;

    private static final Random random = new Random();

    @BeforeEach
    public void setup() {
        when(mockPluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME)).thenReturn(successItemsCounter);
        when(mockPluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME)).thenReturn(failureItemsCounter);
        when(mockPluginMetrics.summary(BYTES_RECEIVED)).thenReturn(bytesReceivedSummary);
        when(mockPluginMetrics.summary(BYTES_PROCESSED)).thenReturn(bytesProcessedSummary);
        when(mockSourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);
        streamWorker = new StreamWorker(mockRecordBufferWriter, mockRecordConverter, mockSourceConfig, mockStreamAcknowledgementManager,
                mockPartitionCheckpoint, mockPluginMetrics, 2, 1000, 10_000, 1_000);
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
        when(col.watch(anyList())).thenReturn(changeStreamIterable);
        when(changeStreamIterable.batchSize(1000)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, true, false);
        ChangeStreamDocument streamDoc1 = mock(ChangeStreamDocument.class);
        ChangeStreamDocument streamDoc2 = mock(ChangeStreamDocument.class);
        Document doc1 = mock(Document.class);
        BsonDocument doc1Key = mock(BsonDocument.class);
        BsonDocument doc2Key = mock(BsonDocument.class);
        BsonDocument bsonDoc1 = new BsonDocument("resumeToken1", new BsonInt32(123));
        BsonDocument bsonDoc2 = new BsonDocument("resumeToken2", new BsonInt32(234));
        when(streamDoc1.getResumeToken()).thenReturn(bsonDoc1);
        when(streamDoc1.getOperationType()).thenReturn(OperationType.INSERT);
        when(streamDoc2.getResumeToken()).thenReturn(bsonDoc2);
        when(streamDoc2.getOperationType()).thenReturn(OperationType.DELETE);
        when(cursor.next())
            .thenReturn(streamDoc1)
            .thenReturn(streamDoc2);
        final String doc1Json1 = UUID.randomUUID().toString();
        final String doc1Json2 = UUID.randomUUID().toString();
        when(doc1Key.get("_id")).thenReturn(new BsonInt64(random.nextLong()));
        when(doc1.toJson(any(JsonWriterSettings.class))).thenReturn(doc1Json1);
        when(doc2Key.get("_id")).thenReturn(new BsonInt32(random.nextInt()));
        when(doc2Key.toJson(any(JsonWriterSettings.class))).thenReturn(doc1Json2);
        when(streamDoc1.getFullDocument()).thenReturn(doc1);
        when(streamDoc1.getDocumentKey()).thenReturn(doc1Key);
        when(streamDoc2.getDocumentKey()).thenReturn(doc2Key);
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
        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus(collection)).thenReturn(Optional.of(s3PartitionStatus));
        when(mockSourceConfig.getIdKey()).thenReturn("docdb_id");
        Event event = mock(Event.class);
        when(event.get("_id", Object.class)).thenReturn(UUID.randomUUID().toString());
        when(mockRecordConverter.convert(anyString(), anyLong(), anyLong(), any(OperationType.class), anyString())).thenReturn(event);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> {
            try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
                mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                        .thenReturn(mongoClient);
                streamWorker.processStream(streamPartition);
            }
        });
        await()
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(() ->  verify(mongoClient).close());
        verify(mongoDatabase).getCollection(eq("collection"));
        verify(mockPartitionCheckpoint).getGlobalS3FolderCreationStatus(collection);
        verify(mockRecordConverter).initializePartitions(partitions);
        verify(mockRecordConverter).convert(eq(doc1Json1), eq(timeSecond1 * 1_000L), eq(timeSecond1 * 1_000_000L), eq(OperationType.INSERT), eq(BsonType.INT64.name()));
        verify(mockRecordConverter).convert(eq(doc1Json2), eq(timeSecond2 * 1_000L), eq(timeSecond2 * 1_000_000L), eq(OperationType.DELETE), eq(BsonType.INT32.name()));
        verify(mockRecordBufferWriter).writeToBuffer(eq(null), any());
        verify(event, times(2)).put(mockSourceConfig.getIdKey(), event.get(DOCUMENTDB_ID_FIELD_NAME, Object.class));
        // doc1Json1 and doc2Json2 are of the same byte size
        verify(bytesReceivedSummary, times(2)).record(doc1Json1.getBytes().length);
        verify(successItemsCounter).increment(2);
        verify(bytesProcessedSummary).record(doc1Json1.getBytes().length + doc1Json2.getBytes().length);
        verify(failureItemsCounter, never()).increment();
        verify(mockPartitionCheckpoint, atLeast(1)).checkpoint("{\"resumeToken2\": 234}", 2);
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
        when(col.watch(anyList())).thenReturn(changeStreamIterable);
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
        when(streamDoc1.getOperationType()).thenReturn(OperationType.INSERT);
        when(streamDoc2.getFullDocument()).thenReturn(doc2);
        when(streamDoc2.getOperationType()).thenReturn(OperationType.INSERT);
        when(streamDoc3.getFullDocument()).thenReturn(doc3);
        when(streamDoc3.getOperationType()).thenReturn(OperationType.INSERT);
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
        when(streamDoc1.getOperationType()).thenReturn(OperationType.INSERT);
        when(streamDoc2.getOperationType()).thenReturn(OperationType.INSERT);
        when(streamDoc3.getOperationType()).thenReturn(OperationType.INSERT);
        final String resumeToken1 = UUID.randomUUID().toString();
        final String resumeToken2 = UUID.randomUUID().toString();
        final String resumeToken3 = UUID.randomUUID().toString();
        when(bsonDoc1.toJson(any(JsonWriterSettings.class))).thenReturn(resumeToken1);
        when(bsonDoc2.toJson(any(JsonWriterSettings.class))).thenReturn(resumeToken2);
        when(bsonDoc3.toJson(any(JsonWriterSettings.class))).thenReturn(resumeToken3);
        S3PartitionStatus s3PartitionStatus = mock(S3PartitionStatus.class);
        final List<String> partitions = List.of("first", "second");
        when(s3PartitionStatus.getPartitions()).thenReturn(partitions);
        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus(collection)).thenReturn(Optional.of(s3PartitionStatus));
        Event event = mock(Event.class);
        when(mockRecordConverter.convert(anyString(), anyLong(), anyLong(), any(OperationType.class), anyString())).thenReturn(event);
        try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {

            mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                    .thenReturn(mongoClient);
            streamWorker.processStream(streamPartition);

        }
        verify(mongoClient, times(1)).close();
        verify(mongoDatabase).getCollection(eq("collection"));
        verify(cursor).close();
        verify(cursor, times(4)).hasNext();
        verify(mockPartitionCheckpoint).getGlobalS3FolderCreationStatus(collection);
        verify(mockPartitionCheckpoint, atLeast(1)).checkpoint(resumeToken3, 3);
        verify(successItemsCounter).increment(1);
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
        when(col.watch(anyList())).thenReturn(changeStreamIterable);
        when(changeStreamIterable.batchSize(1000)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.iterator()).thenReturn(cursor);
        S3PartitionStatus s3PartitionStatus = mock(S3PartitionStatus.class);
        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus(collection)).thenReturn(Optional.of(s3PartitionStatus));
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

    @Test
    void test_processStream_terminateChangeStreamSuccess() {
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
        when(col.watch(anyList())).thenReturn(changeStreamIterable);
        when(changeStreamIterable.batchSize(1000)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, true, true);
        ChangeStreamDocument streamDoc1 = mock(ChangeStreamDocument.class);
        ChangeStreamDocument streamDoc2 = mock(ChangeStreamDocument.class);
        ChangeStreamDocument streamDoc3 = mock(ChangeStreamDocument.class);
        BsonDocument keyDoc1 = mock(BsonDocument.class);
        Document doc1 = mock(Document.class);
        BsonDocument bsonDoc1 = new BsonDocument("resumeToken1", new BsonInt32(123));
        when(streamDoc1.getResumeToken()).thenReturn(bsonDoc1);
        when(streamDoc1.getOperationType()).thenReturn(OperationType.INSERT);
        when(streamDoc2.getOperationType()).thenReturn(OperationType.OTHER);
        when(streamDoc3.getOperationType()).thenReturn(OperationType.DROP);
        when(cursor.next())
                .thenReturn(streamDoc1, streamDoc2, streamDoc3);
        final String doc1Json1 = UUID.randomUUID().toString();
        when(doc1.toJson(any(JsonWriterSettings.class))).thenReturn(doc1Json1);
        when(streamDoc1.getFullDocument()).thenReturn(doc1);
        when(streamDoc1.getDocumentKey()).thenReturn(keyDoc1);
        when(keyDoc1.get("_id")).thenReturn(new BsonBoolean(random.nextBoolean()));
        final OperationType operationType1 = OperationType.INSERT;
        when(streamDoc1.getOperationType()).thenReturn(operationType1);
        final BsonTimestamp bsonTimestamp1 = mock(BsonTimestamp.class);
        final int timeSecond1 = random.nextInt();
        when(bsonTimestamp1.getTime()).thenReturn(timeSecond1);
        when(streamDoc1.getClusterTime()).thenReturn(bsonTimestamp1);
        S3PartitionStatus s3PartitionStatus = mock(S3PartitionStatus.class);
        final List<String> partitions = List.of("first", "second");
        when(s3PartitionStatus.getPartitions()).thenReturn(partitions);
        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus(collection)).thenReturn(Optional.of(s3PartitionStatus));
        Event event = mock(Event.class);
        when(mockRecordConverter.convert(anyString(), anyLong(), anyLong(), any(OperationType.class), anyString())).thenReturn(event);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
                mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                        .thenReturn(mongoClient);
                streamWorker.processStream(streamPartition);
            }
        });
        await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->  verify(mongoClient).close());
        verify(mongoDatabase).getCollection(eq("collection"));
        verify(mockPartitionCheckpoint).getGlobalS3FolderCreationStatus(collection);
        verify(mockRecordConverter).initializePartitions(partitions);
        verify(mockRecordConverter).convert(eq(doc1Json1), eq(timeSecond1 * 1_000L), eq(timeSecond1 * 1_000_000L), eq(operationType1), eq(BsonType.BOOLEAN.name()));
        verify(mockRecordBufferWriter).writeToBuffer(eq(null), any());
        verify(successItemsCounter).increment(1);
        verify(failureItemsCounter, never()).increment();
        verify(mockPartitionCheckpoint).resetCheckpoint();
    }

    @ParameterizedTest
    @MethodSource("mongoDataTypeProvider")
    void test_processStream_dataTypeConversionSuccess(final String actualDocument, final BsonValue bsonValue, final String expectedDocument) {
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
        when(col.watch(anyList())).thenReturn(changeStreamIterable);
        when(changeStreamIterable.batchSize(1000)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP)).thenReturn(changeStreamIterable);
        when(changeStreamIterable.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        ChangeStreamDocument streamDoc1 = mock(ChangeStreamDocument.class);
        BsonDocument key1 = mock(BsonDocument.class);
        Document doc1 = Document.parse(actualDocument);
        BsonDocument bsonDoc1 = new BsonDocument("resumeToken1", new BsonInt32(123));
        when(streamDoc1.getResumeToken()).thenReturn(bsonDoc1);
        when(streamDoc1.getOperationType()).thenReturn(OperationType.INSERT);
        when(cursor.next())
            .thenReturn(streamDoc1);
        when(key1.get("_id")).thenReturn(bsonValue);
        when(streamDoc1.getDocumentKey()).thenReturn(key1);
        when(streamDoc1.getFullDocument()).thenReturn(doc1);
        final OperationType operationType1 = OperationType.INSERT;
        when(streamDoc1.getOperationType()).thenReturn(operationType1);
        final BsonTimestamp bsonTimestamp1 = mock(BsonTimestamp.class);
        final int timeSecond1 = random.nextInt();
        when(bsonTimestamp1.getTime()).thenReturn(timeSecond1);
        when(streamDoc1.getClusterTime()).thenReturn(bsonTimestamp1);
        S3PartitionStatus s3PartitionStatus = mock(S3PartitionStatus.class);
        final List<String> partitions = List.of("first", "second");
        when(s3PartitionStatus.getPartitions()).thenReturn(partitions);
        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus(collection)).thenReturn(Optional.of(s3PartitionStatus));
        Event event = mock(Event.class);
        when(mockRecordConverter.convert(anyString(), anyLong(), anyLong(), any(OperationType.class), anyString())).thenReturn(event);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
                mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                        .thenReturn(mongoClient);
                streamWorker.processStream(streamPartition);
            }
        });
        await()
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(() ->  verify(mongoClient).close());
        verify(mongoDatabase).getCollection(eq("collection"));
        verify(mockPartitionCheckpoint).getGlobalS3FolderCreationStatus(collection);
        verify(mockRecordConverter).initializePartitions(partitions);
        verify(mockRecordConverter).convert(eq(expectedDocument), eq(timeSecond1 * 1_000L), eq(timeSecond1 * 1_000_000L), eq(operationType1), eq(bsonValue.getBsonType().name()));
        verify(mockRecordBufferWriter).writeToBuffer(eq(null), any());
        verify(successItemsCounter).increment(1);
        verify(failureItemsCounter, never()).increment();
        verify(mockPartitionCheckpoint).resetCheckpoint();
    }

    private static Stream<Arguments> mongoDataTypeProvider() {
        return Stream.of(
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"name\": \"Hello User\"}",
                        new BsonBoolean(random.nextBoolean()),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"name\": \"Hello User\"}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"nullField\": null}",
                        new BsonArray(),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"nullField\": null}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"numberField\": 123}",
                        new BsonDateTime(Math.abs(random.nextLong())),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"numberField\": 123}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"doubleValue\": 3.14159}",
                        new BsonTimestamp(Math.abs(random.nextLong())),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"doubleValue\": 3.14159}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"longValue\": { \"$numberLong\": \"1234567890123456768\"}}",
                        new BsonObjectId(new ObjectId()),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"longValue\": 1234567890123456768}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"stringField\": \"Hello, Mongo!\"}",
                        new BsonDocument(),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"stringField\": \"Hello, Mongo!\"}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"booleanField\": true}",
                        new BsonDouble(random.nextDouble()),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"booleanField\": true}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"dateField\": { \"$date\": \"2024-05-03T13:57:51.155Z\"}}",
                        new BsonDecimal128(new Decimal128(random.nextLong())),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"dateField\": 1714744671155}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"arrayField\": [\"a\",\"b\",\"c\"]}",
                        new BsonUndefined(),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"arrayField\": [\"a\", \"b\", \"c\"]}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"objectField\": { \"nestedKey\": \"nestedValue\"}}",
                        new BsonDocument(),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"objectField\": {\"nestedKey\": \"nestedValue\"}}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"binaryField\": { \"$binary\": {\"base64\": \"AQIDBA==\", \"subType\": \"00\"}}}",
                        new BsonString(UUID.randomUUID().toString()),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"binaryField\": \"AQIDBA==\"}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"objectIdField\": { \"$oid\": \"6634ed693ac62386d57b12d0\" }}",
                        new BsonString(UUID.randomUUID().toString()),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"objectIdField\": \"6634ed693ac62386d57b12d0\"}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"timestampField\": { \"$timestamp\": {\"t\": 1714744681, \"i\": 29}}}",
                        new BsonObjectId(new ObjectId()),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"timestampField\": 7364772325884952605}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"regexField\": { \"$regularExpression\": {\"pattern\": \"^ABC\", \"options\": \"i\"}}}",
                        new BsonObjectId(new ObjectId()),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"regexField\": {\"pattern\": \"^ABC\", \"options\": \"i\"}}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"minKeyField\": { \"$minKey\": 1}}",
                        new BsonObjectId(new ObjectId()),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"minKeyField\": null}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"maxKeyField\": { \"$maxKey\": 1}}",
                        new BsonObjectId(new ObjectId()),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"maxKeyField\": null}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"bigDecimalField\": { \"$numberDecimal\": \"123456789.0123456789\"}}",
                        new BsonObjectId(new ObjectId()),
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"bigDecimalField\": \"123456789.0123456789\"}")
        );
    }
}
