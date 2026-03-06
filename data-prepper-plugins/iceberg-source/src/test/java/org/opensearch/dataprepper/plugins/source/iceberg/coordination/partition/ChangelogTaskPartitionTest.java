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
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ChangelogTaskProgressState;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChangelogTaskPartitionTest {

    @Test
    void getPartitionType_returnsChangelogTask() {
        final ChangelogTaskProgressState state = new ChangelogTaskProgressState();
        final ChangelogTaskPartition partition = new ChangelogTaskPartition("key", state);
        assertThat(partition.getPartitionType(), equalTo("CHANGELOG_TASK"));
    }

    @Test
    void getPartitionKey_returnsProvidedKey() {
        final ChangelogTaskProgressState state = new ChangelogTaskProgressState();
        final ChangelogTaskPartition partition = new ChangelogTaskPartition("test-key", state);
        assertThat(partition.getPartitionKey(), equalTo("test-key"));
    }

    @Test
    void getProgressState_returnsProvidedState() {
        final ChangelogTaskProgressState state = new ChangelogTaskProgressState();
        state.setSnapshotId(100L);
        state.setTableName("db.table1");
        final ChangelogTaskPartition partition = new ChangelogTaskPartition("key", state);
        assertThat(partition.getProgressState().isPresent(), equalTo(true));
        assertThat(partition.getProgressState().get().getSnapshotId(), equalTo(100L));
        assertThat(partition.getProgressState().get().getTableName(), equalTo("db.table1"));
    }

    @Test
    void getProgressState_fromStoreItem_returnsRestoredState() {
        final SourcePartitionStoreItem item = mock(SourcePartitionStoreItem.class);
        when(item.getSourceIdentifier()).thenReturn("prefix|CHANGELOG_TASK");
        when(item.getSourcePartitionKey()).thenReturn("db.table1|snap|uuid");
        when(item.getPartitionProgressState()).thenReturn(
                "{\"snapshotId\":100,\"tableName\":\"db.table1\",\"loadedRecords\":0,\"totalRecords\":50," +
                "\"dataFilePaths\":[\"/path/file1.parquet\"],\"taskTypes\":[\"ADDED\"]}");

        final ChangelogTaskPartition partition = new ChangelogTaskPartition(item);
        assertThat(partition.getPartitionKey(), equalTo("db.table1|snap|uuid"));
        assertThat(partition.getProgressState().isPresent(), equalTo(true));

        final ChangelogTaskProgressState state = partition.getProgressState().get();
        assertThat(state.getSnapshotId(), equalTo(100L));
        assertThat(state.getTableName(), equalTo("db.table1"));
        assertThat(state.getTotalRecords(), equalTo(50L));
        assertThat(state.getDataFilePaths(), equalTo(List.of("/path/file1.parquet")));
        assertThat(state.getTaskTypes(), equalTo(List.of("ADDED")));
    }
}
