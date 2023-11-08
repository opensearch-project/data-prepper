/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.CredentialsConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MongoDBSnapshotWorkerTest {
    @Mock
    private SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator;
    @Mock
    private Buffer<Record<Object>> buffer;
    @Mock
    private MongoDBPartitionCreationSupplier mongoDBPartitionCreationSupplier;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private MongoDBConfig mongoDBConfig;
    @Mock
    private SourcePartition<MongoDBSnapshotProgressState> sourcePartition;
    @Mock
    private Counter counter;
    private MongoDBSnapshotWorker testWorker;
    private ExecutorService executorService;


    @BeforeEach
    public void setup() throws TimeoutException {
        lenient().when(mongoDBConfig.getExportConfig()).thenReturn(new MongoDBConfig.ExportConfig());
        lenient().when(mongoDBConfig.getCredentialsConfig()).thenReturn(new CredentialsConfig(new CredentialsConfig.PlainText("user", "user"), null));
        lenient().when(buffer.isByteBuffer()).thenReturn(false);
        lenient().doNothing().when(buffer).write(any(), anyInt());
        lenient().doNothing().when(sourceCoordinator).saveProgressStateForPartition(anyString(), any());
        lenient().when(pluginMetrics.counter(anyString())).thenReturn(counter);
        executorService = Executors.newSingleThreadExecutor();
        testWorker = new MongoDBSnapshotWorker(sourceCoordinator, buffer, mongoDBPartitionCreationSupplier, pluginMetrics, acknowledgementSetManager, mongoDBConfig);
    }

    @Test
    public void test_shouldSleepIfNoPartitionRetrieved() throws InterruptedException {
        when(sourceCoordinator.getNextPartition(mongoDBPartitionCreationSupplier)).thenReturn(Optional.empty());
        final Future<?> future = executorService.submit(() -> testWorker.run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));
        assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS), equalTo(true));
    }

    @ParameterizedTest
    @CsvSource({
        "test.collection|0|1|java.lang.Integer",
        "test.collection|0|1|java.lang.Double",
        "test.collection|0|1|java.lang.String",
        "test.collection|0|1|java.lang.Long",
        "test.collection|000000000000000000000000|000000000000000000000001|org.bson.types.ObjectId"
    })
    public void test_shouldProcessPartitionSuccess(final String partitionKey) throws InterruptedException, TimeoutException {
        this.mockDependencyAndProcessPartition(partitionKey, true);

        final ArgumentCaptor<Record<Object>> ingestDataCapture = ArgumentCaptor.forClass(Record.class);
        verify(buffer, times(2)).write(ingestDataCapture.capture(), anyInt());
        List<Record<Object>> capturedData = ingestDataCapture.getAllValues();
        String data1 = ((Event) capturedData.get(0).getData()).jsonBuilder().includeTags(null).toJsonString();
        String data2 = ((Event) capturedData.get(1).getData()).jsonBuilder().includeTags(null).toJsonString();
        assertThat(data1, is("{\"_id\":0,\"__source_db\":\"test\",\"__collection\":\"collection\",\"__op\":\"create\"}"));
        assertThat(data2, is("{\"_id\":1,\"__source_db\":\"test\",\"__collection\":\"collection\",\"__op\":\"create\"}"));
    }

    @Test
    public void test_shouldProcessPartitionSuccess_byteBuffer() throws Exception {
        when(buffer.isByteBuffer()).thenReturn(true);
        doNothing().when(buffer).writeBytes(any(byte[].class), any(), anyInt());
        this.mockDependencyAndProcessPartition("test.collection|0|1|java.lang.Integer", true);

        final ArgumentCaptor<byte[]> ingestDataCapture = ArgumentCaptor.forClass(byte[].class);
        verify(buffer, times(2)).writeBytes(ingestDataCapture.capture(), any(), anyInt());
        List<byte[]> capturedData = ingestDataCapture.getAllValues();
        String data1 = new String(capturedData.get(0));
        String data2 = new String(capturedData.get(1));
        assertThat(data1, is("{\"_id\":0,\"__source_db\":\"test\",\"__collection\":\"collection\",\"__op\":\"create\"}"));
        assertThat(data2, is("{\"_id\":1,\"__source_db\":\"test\",\"__collection\":\"collection\",\"__op\":\"create\"}"));
    }

    @Test
    public void test_shouldProcessPartitionSuccess_ackEnabled() throws InterruptedException, TimeoutException {
        MongoDBConfig.ExportConfig exportConfig = mock(MongoDBConfig.ExportConfig.class);
        when(exportConfig.getAcknowledgements()).thenReturn(true);
        when(mongoDBConfig.getExportConfig()).thenReturn(exportConfig);
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        doAnswer(invocation -> {
            Consumer<Boolean> consumer = invocation.getArgument(0);
            consumer.accept(true);
            return acknowledgementSet;
        }).when(acknowledgementSetManager).create(any(Consumer.class), any());
        doNothing().when(sourceCoordinator).updatePartitionForAcknowledgmentWait(anyString(), any());

        this.mockDependencyAndProcessPartition("test.collection|0|1|java.lang.Integer", true);

        final ArgumentCaptor<Record<Object>> ingestDataCapture = ArgumentCaptor.forClass(Record.class);
        verify(buffer, times(2)).write(ingestDataCapture.capture(), anyInt());
        List<Record<Object>> capturedData = ingestDataCapture.getAllValues();
        String data1 = ((Event) capturedData.get(0).getData()).jsonBuilder().includeTags(null).toJsonString();
        String data2 = ((Event) capturedData.get(1).getData()).jsonBuilder().includeTags(null).toJsonString();
        assertThat(data1, is("{\"_id\":0,\"__source_db\":\"test\",\"__collection\":\"collection\",\"__op\":\"create\"}"));
        assertThat(data2, is("{\"_id\":1,\"__source_db\":\"test\",\"__collection\":\"collection\",\"__op\":\"create\"}"));
    }

    @Test
    public void test_shouldGiveUpPartitionIfExceptionOccurred() throws InterruptedException {
        doNothing().when(sourceCoordinator).giveUpPartitions();
        this.mockDependencyAndProcessPartition("invalidPartition", false);
        verify(sourceCoordinator, times(1)).giveUpPartitions();
    }

    @Test
    public void test_shouldThreadSleepIfExceptionOccurred() throws InterruptedException {
        doThrow(new RuntimeException("")).when(sourceCoordinator).getNextPartition(mongoDBPartitionCreationSupplier);
        final Future<?> future = executorService.submit(() -> testWorker.run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));
        assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS), equalTo(true));
    }

    private void mockDependencyAndProcessPartition(String partitionKey, boolean shouldProcessSucceed) throws InterruptedException {
        lenient().when(sourcePartition.getPartitionKey()).thenReturn(partitionKey);
        lenient().doNothing().when(sourceCoordinator).completePartition(anyString(), anyBoolean());
        lenient().when(sourceCoordinator.getNextPartition(mongoDBPartitionCreationSupplier))
                .thenReturn(Optional.of(sourcePartition))
                .thenReturn(Optional.empty());

        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoCollection col = mock(MongoCollection.class);
        FindIterable findIterable = mock(FindIterable.class);
        MongoCursor cursor = mock(MongoCursor.class);
        lenient().when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        lenient().when(mongoDatabase.getCollection(anyString())).thenReturn(col);
        lenient().when(col.find(any(Bson.class))).thenReturn(findIterable);
        lenient().when(findIterable.iterator()).thenReturn(cursor);
        lenient().when(cursor.hasNext()).thenReturn(true, true, false);
        lenient().when(cursor.next())
                .thenReturn(new Document("_id", 0))
                .thenReturn(new Document("_id", 1));

        final Future<?> future = executorService.submit(() -> {
            try (MockedStatic<MongoClients> mockedMongoClientsStatic = mockStatic(MongoClients.class)) {
                mockedMongoClientsStatic.when(() -> MongoClients.create(anyString())).thenReturn(mongoClient);
                testWorker.run();
            }
        });
        Thread.sleep(1000);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));
        assertThat(executorService.awaitTermination(1000, TimeUnit.MILLISECONDS), equalTo(true));

        if (shouldProcessSucceed) {
            // Verify Results
            verify(cursor, times(2)).next();

            final ArgumentCaptor<MongoDBSnapshotProgressState> progressStateCapture = ArgumentCaptor.forClass(MongoDBSnapshotProgressState.class);
            verify(sourceCoordinator, times(1)).saveProgressStateForPartition(eq(partitionKey), progressStateCapture.capture());
            List<MongoDBSnapshotProgressState> progressStates = progressStateCapture.getAllValues();
            assertThat(progressStates.get(0).getTotal(), is(2));
        }
    }
}

