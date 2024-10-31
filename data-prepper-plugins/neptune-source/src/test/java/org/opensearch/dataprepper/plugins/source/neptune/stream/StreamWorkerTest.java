package org.opensearch.dataprepper.plugins.source.neptune.stream;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.neptune.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.neptune.model.S3PartitionStatus;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamWorker.BYTES_PROCESSED;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamWorker.BYTES_RECEIVED;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamWorker.FAILURE_ITEM_COUNTER_NAME;
import static org.opensearch.dataprepper.plugins.source.neptune.stream.StreamWorker.SUCCESS_ITEM_COUNTER_NAME;

@ExtendWith(MockitoExtension.class)
public class StreamWorkerTest {
    @Mock
    private RecordBufferWriter mockRecordBufferWriter;
    @Mock
    private StreamRecordConverter mockRecordConverter;
    @Mock
    private NeptuneSourceConfig mockSourceConfig;
    @Mock
    private StreamAcknowledgementManager mockStreamAcknowledgementManager;
    @Mock
    private DataStreamPartitionCheckpoint mockPartitionCheckpoint;
    @Mock
    private PluginMetrics mockPluginMetrics;

    @Mock
    private AwsConfig mockAwsConfig;
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
    private Counter streamApiInvocations;

    @Mock
    private Counter stream4xxErrors;

    @Mock
    private Counter stream5xxErrors;

    private StreamWorker streamWorker;

    private static final Random random = new Random();

    @BeforeEach
    public void setup() {
        when(mockPluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME)).thenReturn(successItemsCounter);
        when(mockPluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME)).thenReturn(failureItemsCounter);
        when(mockPluginMetrics.summary(BYTES_RECEIVED)).thenReturn(bytesReceivedSummary);
        when(mockPluginMetrics.summary(BYTES_PROCESSED)).thenReturn(bytesProcessedSummary);
        when(mockSourceConfig.isAcknowledgments()).thenReturn(false);
        streamWorker = new StreamWorker(mockRecordBufferWriter, mockRecordConverter, mockSourceConfig, mockStreamAcknowledgementManager,
                mockPartitionCheckpoint, mockPluginMetrics, 2, 1000, 10_000, 1_000);
    }

    @Test
    void test_processStream_invalidCollection() {
        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus()).thenReturn(Optional.of(new S3PartitionStatus(List.of("part1"))));
        when(mockSourceConfig.getRegion()).thenReturn("us-east-1");
        when(mockSourceConfig.getAwsConfig()).thenReturn(mockAwsConfig);
        when(mockAwsConfig.getAwsStsRoleArn()).thenReturn("rolearn");
        when(mockAwsConfig.getAwsStsExternalId()).thenReturn("externalid");
        assertThrows(IllegalArgumentException.class, () -> streamWorker.processStream(streamPartition));
//        verify(streamApiInvocations).increment();
//        verify(stream4xxErrors).increment();
//        verify(stream5xxErrors, never()).increment();
    }

//    @ParameterizedTest
//    @MethodSource("mongoDataTypeProvider")
//    void test_processStream_dataTypeConversionSuccess(final String actualDocument, final String expectedDocument) throws IOException {
//        lenient().when(mockAwsConfig.getAwsStsExternalId()).thenReturn("externalid");
//        streamWorker.processStream(streamPartition);
//        final String collection = "database.collection";
//        when(streamProgressState.shouldWaitForExport()).thenReturn(false);
//        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
//        when(streamPartition.getCollection()).thenReturn(collection);
//        MongoClient mongoClient = mock(MongoClient.class);
//        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
//        MongoCollection col = mock(MongoCollection.class);
//        ChangeStreamIterable changeStreamIterable = mock(ChangeStreamIterable.class);
//        MongoCursor cursor = mock(MongoCursor.class);
//        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
//        when(mongoDatabase.getCollection(anyString())).thenReturn(col);
//        when(col.watch(anyList())).thenReturn(changeStreamIterable);
//        when(changeStreamIterable.batchSize(1000)).thenReturn(changeStreamIterable);
//        when(changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP)).thenReturn(changeStreamIterable);
//        when(changeStreamIterable.iterator()).thenReturn(cursor);
//        when(cursor.hasNext()).thenReturn(true, false);
//        ChangeStreamDocument streamDoc1 = mock(ChangeStreamDocument.class);
//        BsonDocument key1 = mock(BsonDocument.class);
//        Document doc1 = Document.parse(actualDocument);
//        BsonDocument bsonDoc1 = new BsonDocument("resumeToken1", new BsonInt32(123));
//        when(streamDoc1.getResumeToken()).thenReturn(bsonDoc1);
//        when(streamDoc1.getOperationType()).thenReturn(OperationType.INSERT);
//        when(cursor.next())
//                .thenReturn(streamDoc1);
//        when(key1.get("_id")).thenReturn(bsonValue);
//        when(streamDoc1.getDocumentKey()).thenReturn(key1);
//        when(streamDoc1.getFullDocument()).thenReturn(doc1);
//        final OperationType operationType1 = OperationType.INSERT;
//        when(streamDoc1.getOperationType()).thenReturn(operationType1);
//        final BsonTimestamp bsonTimestamp1 = mock(BsonTimestamp.class);
//        final int timeSecond1 = random.nextInt();
//        when(bsonTimestamp1.getTime()).thenReturn(timeSecond1);
//        when(streamDoc1.getClusterTime()).thenReturn(bsonTimestamp1);
//        S3PartitionStatus s3PartitionStatus = mock(S3PartitionStatus.class);
//        final List<String> partitions = List.of("first", "second");
//        when(s3PartitionStatus.getPartitions()).thenReturn(partitions);
//        when(mockPartitionCheckpoint.getGlobalS3FolderCreationStatus(collection)).thenReturn(Optional.of(s3PartitionStatus));
//        Event event = mock(Event.class);
//        when(mockRecordConverter.convert(anyString(), anyLong(), anyLong(), any(OperationType.class), anyString())).thenReturn(event);
//        final ExecutorService executorService = Executors.newSingleThreadExecutor();
//        executorService.submit(() -> {
//            try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
//                mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
//                        .thenReturn(mongoClient);
//                streamWorker.processStream(streamPartition);
//            }
//        });
//        await()
//                .atMost(Duration.ofSeconds(10))
//                .untilAsserted(() -> verify(mongoClient).close());
//        verify(mongoDatabase).getCollection(eq("collection"));
//        verify(mockPartitionCheckpoint).getGlobalS3FolderCreationStatus(collection);
//        verify(mockRecordConverter).initializePartitions(partitions);
//        verify(mockRecordConverter).convert(eq(expectedDocument), eq(timeSecond1 * 1_000L), eq(timeSecond1 * 1_000_000L), eq(operationType1), eq(bsonValue.getBsonType().name()));
//        verify(mockRecordBufferWriter).writeToBuffer(eq(null), any());
//        verify(successItemsCounter).increment(1);
//        verify(failureItemsCounter, never()).increment();
//        verify(mockPartitionCheckpoint).resetCheckpoint();
//        verify(streamApiInvocations).increment();
//        verify(stream4xxErrors, never()).increment();
//        verify(stream5xxErrors, never()).increment();
//    }
//
//    private static Stream<Arguments> mongoDataTypeProvider() {
//        return Stream.of(
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"name\": \"Hello User\"}",
//                        new BsonBoolean(random.nextBoolean()),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"name\": \"Hello User\"}"
//                ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"nullField\": null}",
//                        new BsonArray(),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"nullField\": null}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"numberField\": 123}",
//                        new BsonDateTime(Math.abs(random.nextLong())),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"numberField\": 123}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"doubleValue\": 3.14159}",
//                        new BsonTimestamp(Math.abs(random.nextLong())),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"doubleValue\": 3.14159}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"longValue\": { \"$numberLong\": \"9223372036854775801\"}}",
//                        new BsonObjectId(new ObjectId()),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"longValue\": 9223372036854775801}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"stringField\": \"Hello, Mongo!\"}",
//                        new BsonDocument(),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"stringField\": \"Hello, Mongo!\"}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"booleanField\": true}",
//                        new BsonDouble(random.nextDouble()),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"booleanField\": true}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"dateField\": { \"$date\": \"2024-05-03T13:57:51.155Z\"}}",
//                        new BsonDecimal128(new Decimal128(random.nextLong())),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"dateField\": 1714744671155}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"arrayField\": [\"a\",\"b\",\"c\"]}",
//                        new BsonUndefined(),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"arrayField\": [\"a\", \"b\", \"c\"]}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"objectField\": { \"nestedKey\": \"nestedValue\"}}",
//                        new BsonDocument(),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"objectField\": {\"nestedKey\": \"nestedValue\"}}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"binaryField\": { \"$binary\": {\"base64\": \"AQIDBA==\", \"subType\": \"00\"}}}",
//                        new BsonString(UUID.randomUUID().toString()),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"binaryField\": \"AQIDBA==\"}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"objectIdField\": { \"$oid\": \"6634ed693ac62386d57b12d0\" }}",
//                        new BsonString(UUID.randomUUID().toString()),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"objectIdField\": \"6634ed693ac62386d57b12d0\"}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"timestampField\": { \"$timestamp\": {\"t\": 1714744681, \"i\": 29}}}",
//                        new BsonObjectId(new ObjectId()),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"timestampField\": 1714744681}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"regexField\": { \"$regularExpression\": {\"pattern\": \"^ABC\", \"options\": \"i\"}}}",
//                        new BsonObjectId(new ObjectId()),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"regexField\": {\"pattern\": \"^ABC\", \"options\": \"i\"}}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"minKeyField\": { \"$minKey\": 1}}",
//                        new BsonObjectId(new ObjectId()),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"minKeyField\": null}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"maxKeyField\": { \"$maxKey\": 1}}",
//                        new BsonObjectId(new ObjectId()),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"maxKeyField\": null}"
//                        ),
//                Arguments.of(
//                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"bigDecimalField\": { \"$numberDecimal\": \"123456789.0123456789\"}}",
//                        new BsonObjectId(new ObjectId()),
//                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"bigDecimalField\": \"123456789.0123456789\"}"
//                        )
//        );
//    }
}
