/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LeaderPartitionTest {

    @Test
    void getPartitionType_newInstance_returnsLeader() {
        final LeaderPartition partition = new LeaderPartition();
        assertThat(partition.getPartitionType(), equalTo("LEADER"));
    }

    @Test
    void getPartitionKey_newInstance_returnsGlobal() {
        final LeaderPartition partition = new LeaderPartition();
        assertThat(partition.getPartitionKey(), equalTo("GLOBAL"));
    }

    @Test
    void getProgressState_newInstance_returnsNotInitialized() {
        final LeaderPartition partition = new LeaderPartition();
        assertThat(partition.getProgressState().isPresent(), equalTo(true));
        assertThat(partition.getProgressState().get().isInitialized(), equalTo(false));
    }

    @Test
    void getProgressState_fromStoreItem_returnsRestoredState() {
        final SourcePartitionStoreItem item = mock(SourcePartitionStoreItem.class);
        when(item.getSourceIdentifier()).thenReturn("prefix|LEADER");
        when(item.getSourcePartitionKey()).thenReturn("GLOBAL");
        when(item.getPartitionProgressState()).thenReturn("{\"initialized\":true,\"last_processed_snapshot_id\":42}");

        final LeaderPartition partition = new LeaderPartition(item);
        assertThat(partition.getProgressState(), notNullValue());
        assertThat(partition.getProgressState().get().isInitialized(), equalTo(true));
        assertThat(partition.getProgressState().get().getLastProcessedSnapshotId(), equalTo(42L));
    }
}
