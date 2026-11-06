/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.mongo.export;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.utils.DocumentDBSourceAggregateMetrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MongoDBExportPartitionSupplier_IsUniformIdTypeTest {

    @Mock
    private MongoDBSourceConfig sourceConfig;
    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;
    @Mock
    private DocumentDBSourceAggregateMetrics aggregateMetrics;
    @Mock
    private MongoCollection<Document> collection;
    @Mock
    private FindIterable<Document> findIterable;

    private MongoDBExportPartitionSupplier supplier;

    @BeforeEach
    void setUp() {
        supplier = new MongoDBExportPartitionSupplier(sourceConfig, sourceCoordinator, aggregateMetrics);
        when(collection.find()).thenReturn(findIterable);
        when(findIterable.projection(any())).thenReturn(findIterable);
        when(findIterable.sort(any())).thenReturn(findIterable);
        when(findIterable.limit(1)).thenReturn(findIterable);
    }

    @Test
    void isUniformIdType_emptyCollection_returnsTrue() {
        when(findIterable.first()).thenReturn(null);
        assertThat(supplier.isUniformIdType(collection), is(true));
    }

    @Test
    void isUniformIdType_uniformObjectId_returnsTrue() {
        when(findIterable.first())
                .thenReturn(new Document("_id", new ObjectId()))
                .thenReturn(new Document("_id", new ObjectId()));
        assertThat(supplier.isUniformIdType(collection), is(true));
    }

    @Test
    void isUniformIdType_uniformString_returnsTrue() {
        when(findIterable.first())
                .thenReturn(new Document("_id", "abc"))
                .thenReturn(new Document("_id", "xyz"));
        assertThat(supplier.isUniformIdType(collection), is(true));
    }

    @Test
    void isUniformIdType_mixedTypes_returnsFalse() {
        when(findIterable.first())
                .thenReturn(new Document("_id", 1))
                .thenReturn(new Document("_id", new ObjectId()));
        assertThat(supplier.isUniformIdType(collection), is(false));
    }

    @Test
    void isUniformIdType_integerAndLong_returnsTrue() {
        when(findIterable.first())
                .thenReturn(new Document("_id", 42))
                .thenReturn(new Document("_id", 999999999999L));
        assertThat(supplier.isUniformIdType(collection), is(true));
    }

    @Test
    void isUniformIdType_doubleAndDecimal128_returnsTrue() {
        when(findIterable.first())
                .thenReturn(new Document("_id", 3.14))
                .thenReturn(new Document("_id", Decimal128.parse("99.99")));
        assertThat(supplier.isUniformIdType(collection), is(true));
    }

    @Test
    void isUniformIdType_doubleAndInteger_returnsTrue() {
        when(findIterable.first())
                .thenReturn(new Document("_id", 3.14))
                .thenReturn(new Document("_id", 42));
        assertThat(supplier.isUniformIdType(collection), is(true));
    }

    @Test
    void isUniformIdType_stringAndObjectId_returnsFalse() {
        when(findIterable.first())
                .thenReturn(new Document("_id", "abc"))
                .thenReturn(new Document("_id", new ObjectId()));
        assertThat(supplier.isUniformIdType(collection), is(false));
    }
}
