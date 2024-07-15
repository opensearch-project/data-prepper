/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.export;

import com.mongodb.MongoClientException;
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
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
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
            // Given a collection with 5000 items which should be split to two partitions: 0-3999 and 4000-4999
            MongoClient mongoClient = mock(MongoClient.class);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            MongoCollection col = mock(MongoCollection.class);
            FindIterable findIterable = mock(FindIterable.class);
            MongoCursor cursor = mock(MongoCursor.class);
            mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                    .thenReturn(mongoClient);
            when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
            when(mongoDatabase.getCollection(anyString())).thenReturn(col);
            when(col.find()).thenReturn(findIterable);
            when(col.find(any(Bson.class))).thenReturn(findIterable);
            when(findIterable.projection(any())).thenReturn(findIterable);
            when(findIterable.sort(any())).thenReturn(findIterable);
            when(findIterable.skip(anyInt())).thenReturn(findIterable);
            when(findIterable.limit(anyInt())).thenReturn(findIterable);
            when(findIterable.iterator()).thenReturn(cursor);
            when(cursor.hasNext()).thenReturn(true, true, false);
            // mock startDoc and endDoc returns, 0-3999, and 4000-4999
            when(cursor.next())
                    .thenReturn(new Document("_id", "0"))
                    .thenReturn(new Document("_id", "4000"));
            when(findIterable.first())
                    .thenReturn(new Document("_id", "3999"))
                    .thenReturn(null)
                    .thenReturn(new Document("_id", "4999"));
            // When Apply Partition create logics
            final PartitionIdentifierBatch partitionIdentifierBatch = testSupplier.apply(exportPartition);
            assertThat(partitionIdentifierBatch.isLastBatch(), is(true));
            assertThat(partitionIdentifierBatch.getEndDocId(), equalTo("4999"));
            List<PartitionIdentifier> partitions = partitionIdentifierBatch.getPartitionIdentifiers();
            // Then dependencies are called
            verify(mongoClient).getDatabase(eq("test"));
            verify(mongoClient, times(1)).close();
            verify(mongoDatabase).getCollection(eq("collection"));
            verify(exportApiInvocations).increment();
            verify(exportPartitionQueryCount, times(2)).increment();
            verify(export4xxErrors, never()).increment();
            verify(export5xxErrors, never()).increment();
            // And partitions are created
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
}