/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ResyncPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PartitionFactoryTest {

    @Mock
    private SourcePartitionStoreItem partitionStoreItem;

    @Test
    void given_leader_partition_item_then_create_leader_partition() {
        PartitionFactory objectUnderTest = createObjectUnderTest();
        when(partitionStoreItem.getSourceIdentifier()).thenReturn(UUID.randomUUID() + "|" + LeaderPartition.PARTITION_TYPE);
        when(partitionStoreItem.getPartitionProgressState()).thenReturn(null);

        assertThat(objectUnderTest.apply(partitionStoreItem), instanceOf(LeaderPartition.class));
    }

    @Test
    void given_export_partition_item_then_create_export_partition() {
        PartitionFactory objectUnderTest = createObjectUnderTest();
        when(partitionStoreItem.getSourceIdentifier()).thenReturn(UUID.randomUUID() + "|" + ExportPartition.PARTITION_TYPE);
        when(partitionStoreItem.getSourcePartitionKey()).thenReturn(UUID.randomUUID() + "|" + UUID.randomUUID());
        when(partitionStoreItem.getPartitionProgressState()).thenReturn(null);

        assertThat(objectUnderTest.apply(partitionStoreItem), instanceOf(ExportPartition.class));
    }

    @Test
    void given_stream_partition_item_then_create_stream_partition() {
        PartitionFactory objectUnderTest = createObjectUnderTest();
        when(partitionStoreItem.getSourceIdentifier()).thenReturn(UUID.randomUUID() + "|" + StreamPartition.PARTITION_TYPE);
        when(partitionStoreItem.getSourcePartitionKey()).thenReturn(UUID.randomUUID().toString());
        when(partitionStoreItem.getPartitionProgressState()).thenReturn(null);

        assertThat(objectUnderTest.apply(partitionStoreItem), instanceOf(StreamPartition.class));
    }

    @Test
    void given_datafile_partition_item_then_create_datafile_partition() {
        PartitionFactory objectUnderTest = createObjectUnderTest();
        when(partitionStoreItem.getSourceIdentifier()).thenReturn(UUID.randomUUID() + "|" + DataFilePartition.PARTITION_TYPE);
        when(partitionStoreItem.getSourcePartitionKey()).thenReturn(UUID.randomUUID() + "|" + UUID.randomUUID() + "|" + UUID.randomUUID());
        when(partitionStoreItem.getPartitionProgressState()).thenReturn(null);

        assertThat(objectUnderTest.apply(partitionStoreItem), instanceOf(DataFilePartition.class));
    }

    @Test
    void given_resync_partition_item_then_create_resync_partition() {
        PartitionFactory objectUnderTest = createObjectUnderTest();
        when(partitionStoreItem.getSourceIdentifier()).thenReturn(UUID.randomUUID() + "|" + ResyncPartition.PARTITION_TYPE);
        when(partitionStoreItem.getSourcePartitionKey()).thenReturn(UUID.randomUUID() + "|" + UUID.randomUUID() + "|12345");
        when(partitionStoreItem.getPartitionProgressState()).thenReturn(null);

        assertThat(objectUnderTest.apply(partitionStoreItem), instanceOf(ResyncPartition.class));
    }

    @Test
    void given_store_item_of_undefined_type_then_create_global_state() {
        PartitionFactory objectUnderTest = createObjectUnderTest();
        when(partitionStoreItem.getSourceIdentifier()).thenReturn(UUID.randomUUID() + "|" + UUID.randomUUID());
        when(partitionStoreItem.getSourcePartitionKey()).thenReturn(UUID.randomUUID().toString());
        when(partitionStoreItem.getPartitionProgressState()).thenReturn(null);

        assertThat(objectUnderTest.apply(partitionStoreItem), instanceOf(GlobalState.class));
    }

    private PartitionFactory createObjectUnderTest() {
        return new PartitionFactory();
    }
}