/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.export;

import com.mongodb.MongoClientException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.micrometer.core.instrument.Counter;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.mongo.model.PartitionIdentifierBatch;
import org.opensearch.dataprepper.plugins.mongo.utils.DocumentDBSourceAggregateMetrics;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

@ExtendWith(MockitoExtension.class)
public class MongoDBExportPartitionSupplierTest {
    private static final String TEST_COLLECTION_NAME = "test.collection";
    @Mock
    private MongoDBSourceConfig mongoDBConfig;

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private DocumentDBSourceAggregateMetrics documentDBSourceAggregateMetrics;

    @Mock
    private CollectionConfig collectionConfig;

    @Mock
    private ExportPartition exportPartition;

    @Mock
    private Counter exportApiInvocations;
    @Mock
    private Counter exportPartitionQueryCount;
    @Mock
    private Counter export4xxErrors;
    @Mock
    private Counter export5xxErrors;

    private MongoDBExportPartitionSupplier testSupplier;

    @BeforeEach
    public void setup() {
        when(exportPartition.getCollection()).thenReturn(TEST_COLLECTION_NAME);
        lenient().when(collectionConfig.getCollectionName()).thenReturn(TEST_COLLECTION_NAME);
        lenient().when(mongoDBConfig.getCollections()).thenReturn(Collections.singletonList(collectionConfig));
        when(documentDBSourceAggregateMetrics.getExportApiInvocations()).thenReturn(exportApiInvocations);
        lenient().when(documentDBSourceAggregateMetrics.getExportPartitionQueryCount()).thenReturn(exportPartitionQueryCount);
        lenient().when(documentDBSourceAggregateMetrics.getExport4xxErrors()).thenReturn(export4xxErrors);
        lenient().when(documentDBSourceAggregateMetrics.getExport5xxErrors()).thenReturn(export5xxErrors);
        testSupplier = new MongoDBExportPartitionSupplier(mongoDBConfig, sourceCoordinator, documentDBSourceAggregateMetrics);
    }

    @Test
    public void test_buildPartitionsCollection() {
        try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
            MongoClient mongoClient = mock(MongoClient.class);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            MongoCollection col = mock(MongoCollection.class);

            mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                    .thenReturn(mongoClient);
            when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
            when(mongoDatabase.getCollection(anyString())).thenReturn(col);

            // Track col.find() calls (no-arg) to distinguish isUniformIdType and lastDoc queries
            final AtomicInteger noArgFindCount = new AtomicInteger(0);
            when(col.find()).thenAnswer((Answer<FindIterable>) invocation -> {
                int callNum = noArgFindCount.incrementAndGet();
                switch (callNum) {
                    case 1: return createFindIterable(new Document("_id", "0"));    // isUniformIdType first
                    case 2: return createFindIterable(new Document("_id", "4999")); // isUniformIdType last
                    case 3: return createFindIterable(new Document("_id", "4999")); // lastDoc when endOfPage is null
                    default: return createFindIterable(null);
                }
            });

            // Track col.find(Bson) calls for partition boundary queries
            final AtomicInteger bsonFindCount = new AtomicInteger(0);
            when(col.find(any(Bson.class))).thenAnswer((Answer<FindIterable>) invocation -> {
                int callNum = bsonFindCount.incrementAndGet();
                switch (callNum) {
                    case 1: return createFindIterable(new Document("_id", "0"));    // 1st start doc
                    case 2: return createFindIterableWithSkip(new Document("_id", "3999")); // 1st end
                    case 3: return createFindIterable(new Document("_id", "4000")); // 2nd start doc
                    case 4: return createFindIterableWithSkip(null);                 // 2nd end -> null
                    default: return createFindIterable(null);
                }
            });

            final PartitionIdentifierBatch partitionIdentifierBatch = testSupplier.apply(exportPartition);
            assertThat(partitionIdentifierBatch.isLastBatch(), is(true));
            assertThat(partitionIdentifierBatch.getEndDocId(), equalTo("4999"));
            List<PartitionIdentifier> partitions = partitionIdentifierBatch.getPartitionIdentifiers();

            verify(mongoClient).getDatabase(eq("test"));
            verify(mongoClient, times(1)).close();
            verify(mongoDatabase).getCollection(eq("collection"));
            verify(exportApiInvocations).increment();
            verify(exportPartitionQueryCount, times(2)).increment();
            verify(export4xxErrors, never()).increment();
            verify(export5xxErrors, never()).increment();

            assertThat(partitions.size(), is(2));
            assertThat(partitions.get(0).getPartitionKey(), is("test.collection|0|3999|java.lang.String|java.lang.String"));
            assertThat(partitions.get(1).getPartitionKey(), is("test.collection|4000|4999|java.lang.String|java.lang.String"));
        }
    }

    @Test
    public void test_buildPartitionsForCollection_error() {
        when(exportPartition.getCollection()).thenReturn("invalidDBName");
        assertThrows(IllegalArgumentException.class, () -> testSupplier.apply(exportPartition));
        verify(exportApiInvocations).increment();
        verify(exportPartitionQueryCount, never()).increment();
        verify(export4xxErrors).increment();
        verify(export5xxErrors, never()).increment();
    }

    @Test
    public void test_buildPartitions_dbException() {
        try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
            mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                    .thenThrow(MongoClientException.class);
            assertThrows(RuntimeException.class, () -> testSupplier.apply(exportPartition));
            verify(exportApiInvocations).increment();
            verify(exportPartitionQueryCount, never()).increment();
            verify(export4xxErrors).increment();
            verify(export5xxErrors, never()).increment();
        }
    }

    private FindIterable createFindIterable(Document result) {
        FindIterable iterable = mock(FindIterable.class);
        when(iterable.projection(any())).thenReturn(iterable);
        when(iterable.sort(any())).thenReturn(iterable);
        when(iterable.limit(anyInt())).thenReturn(iterable);
        when(iterable.first()).thenReturn(result);
        return iterable;
    }

    private FindIterable createFindIterableWithSkip(Document result) {
        FindIterable iterable = mock(FindIterable.class);
        when(iterable.projection(any())).thenReturn(iterable);
        when(iterable.sort(any())).thenReturn(iterable);
        when(iterable.skip(anyInt())).thenReturn(iterable);
        when(iterable.limit(anyInt())).thenReturn(iterable);
        when(iterable.first()).thenReturn(result);
        return iterable;
    }
}
