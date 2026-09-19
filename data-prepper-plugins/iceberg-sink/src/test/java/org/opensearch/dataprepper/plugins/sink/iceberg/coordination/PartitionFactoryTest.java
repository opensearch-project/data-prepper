/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg.coordination;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.partition.WriteResultPartition;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PartitionFactoryTest {

    private final PartitionFactory factory = new PartitionFactory();

    private SourcePartitionStoreItem createItem(final String partitionType) {
        final SourcePartitionStoreItem item = mock(SourcePartitionStoreItem.class);
        when(item.getSourceIdentifier()).thenReturn("pipeline|" + partitionType);
        when(item.getSourcePartitionKey()).thenReturn("test-key");
        when(item.getPartitionProgressState()).thenReturn(null);
        return item;
    }

    @Test
    void apply_leader_partition() {
        final EnhancedSourcePartition result = factory.apply(createItem(LeaderPartition.PARTITION_TYPE));
        assertTrue(result instanceof LeaderPartition);
    }

    @Test
    void apply_write_result_partition() {
        final EnhancedSourcePartition result = factory.apply(createItem(WriteResultPartition.PARTITION_TYPE));
        assertTrue(result instanceof WriteResultPartition);
    }

    @Test
    void apply_unknown_type_throws() {
        assertThrows(IllegalArgumentException.class, () -> factory.apply(createItem("UNKNOWN")));
    }
}
