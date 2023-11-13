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
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.CredentialsConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MongoDBPartitionCreationSupplierTest {
    private static String TEST_COLLECTION_NAME = "test.collection";
    @Mock
    private MongoDBConfig mongoDBConfig;

    @Mock
    private MongoDBConfig.CollectionConfig collectionConfig;

    private MongoDBPartitionCreationSupplier testSupplier;

    @BeforeEach
    public void setup() {
        mongoDBConfig = mock(MongoDBConfig.class);
        collectionConfig = mock(MongoDBConfig.CollectionConfig.class);
        lenient().when(collectionConfig.getCollectionName()).thenReturn(TEST_COLLECTION_NAME);
        lenient().when(mongoDBConfig.getCollections()).thenReturn(Collections.singletonList(collectionConfig));
        lenient().when(mongoDBConfig.getCredentialsConfig()).thenReturn(new CredentialsConfig(new CredentialsConfig.PlainText("user", "user"), null));
        lenient().when(mongoDBConfig.getExportConfig()).thenReturn(new MongoDBConfig.ExportConfig());
        testSupplier = new MongoDBPartitionCreationSupplier(mongoDBConfig);
    }

    @Test
    public void test_returnEmptyPartitionListIfAlreadyPartitioned() {
        final Map<String, Object> globalStateMap = new HashMap<>();
        final Map<String, Object> partitionedCollections = new HashMap<>();
        partitionedCollections.put(TEST_COLLECTION_NAME, Instant.now().toEpochMilli());
        globalStateMap.put(MongoDBPartitionCreationSupplier.GLOBAL_STATE_PARTITIONED_COLLECTION_KEY, partitionedCollections);
        List<PartitionIdentifier> partitions = testSupplier.apply(globalStateMap);
        assert (partitions.isEmpty());
    }

    @Test
    public void test_returnPartitionsForCollection() {
        try (MockedStatic<MongoClients> mockedMongoClientsStatic = mockStatic(MongoClients.class)) {
            // Given a collection with 5000 items which should be split to two partitions: 0-3999 and 4000-4999
            MongoClient mongoClient = mock(MongoClient.class);
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            MongoCollection col = mock(MongoCollection.class);
            FindIterable findIterable = mock(FindIterable.class);
            MongoCursor cursor = mock(MongoCursor.class);
            mockedMongoClientsStatic.when(() -> MongoClients.create(anyString())).thenReturn(mongoClient);
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
            final Map<String, Object> globalStateMap = new HashMap<>();
            List<PartitionIdentifier> partitions = testSupplier.apply(globalStateMap);
            // Then dependencies are called
            verify(mongoClient).getDatabase(eq("test"));
            verify(mongoDatabase).getCollection(eq("collection"));
            // And partitions are created
            assertThat(partitions.size(), is(2));
            assertThat(partitions.get(0).getPartitionKey(), is("test.collection|0|3999|java.lang.String"));
            assertThat(partitions.get(1).getPartitionKey(), is("test.collection|4000|4999|java.lang.String"));
        }
    }

    @Test
    public void test_returnPartitionsForCollection_error() {
        when(collectionConfig.getCollectionName()).thenReturn("invalidDBName");
        final Map<String, Object> globalStateMap = new HashMap<>();
        assertThrows(IllegalArgumentException.class, () -> testSupplier.apply(globalStateMap));
    }
}
