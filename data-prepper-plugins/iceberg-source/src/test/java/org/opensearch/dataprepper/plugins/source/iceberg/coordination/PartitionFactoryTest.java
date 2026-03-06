/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.coordination;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ChangelogTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.InitialLoadTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.LeaderPartition;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PartitionFactoryTest {

    @Mock
    private SourcePartitionStoreItem sourcePartitionStoreItem;

    private final PartitionFactory partitionFactory = new PartitionFactory();

    @Test
    void apply_leaderPartition_returnsLeaderPartition() {
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn("prefix|LEADER");
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn("{\"initialized\":false}");

        final EnhancedSourcePartition result = partitionFactory.apply(sourcePartitionStoreItem);
        assertThat(result, instanceOf(LeaderPartition.class));
    }

    @Test
    void apply_changelogTaskPartition_returnsChangelogTaskPartition() {
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn("prefix|CHANGELOG_TASK");
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn("table|123|uuid");
        when(sourcePartitionStoreItem.getPartitionProgressState())
                .thenReturn("{\"snapshot_id\":123,\"table_name\":\"test\",\"loaded_records\":0,\"total_records\":10,\"data_file_paths\":[\"path1\"],\"task_types\":[\"ADDED\"]}");

        final EnhancedSourcePartition result = partitionFactory.apply(sourcePartitionStoreItem);
        assertThat(result, instanceOf(ChangelogTaskPartition.class));
    }

    @Test
    void apply_initialLoadTaskPartition_returnsInitialLoadTaskPartition() {
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn("prefix|INITIAL_LOAD_TASK");
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn("table|initial|uuid");
        when(sourcePartitionStoreItem.getPartitionProgressState())
                .thenReturn("{\"snapshot_id\":456,\"table_name\":\"test\",\"data_file_path\":\"path1\",\"total_records\":100}");

        final EnhancedSourcePartition result = partitionFactory.apply(sourcePartitionStoreItem);
        assertThat(result, instanceOf(InitialLoadTaskPartition.class));
    }

    @Test
    void apply_unknownType_returnsGlobalState() {
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn("prefix|UNKNOWN");
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn("some-key");
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(null);

        final EnhancedSourcePartition result = partitionFactory.apply(sourcePartitionStoreItem);
        assertThat(result, instanceOf(GlobalState.class));
    }
}
