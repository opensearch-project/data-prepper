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
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.InitialLoadTaskProgressState;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InitialLoadTaskPartitionTest {

    @Test
    void getPartitionType_returnsInitialLoadTask() {
        final InitialLoadTaskProgressState state = new InitialLoadTaskProgressState();
        final InitialLoadTaskPartition partition = new InitialLoadTaskPartition("key", state);
        assertThat(partition.getPartitionType(), equalTo("INITIAL_LOAD_TASK"));
    }

    @Test
    void getPartitionKey_returnsProvidedKey() {
        final InitialLoadTaskProgressState state = new InitialLoadTaskProgressState();
        final InitialLoadTaskPartition partition = new InitialLoadTaskPartition("test-key", state);
        assertThat(partition.getPartitionKey(), equalTo("test-key"));
    }

    @Test
    void getProgressState_returnsProvidedState() {
        final InitialLoadTaskProgressState state = new InitialLoadTaskProgressState();
        state.setSnapshotId(200L);
        state.setTableName("db.table1");
        state.setDataFilePath("/path/file.parquet");
        state.setTotalRecords(1000L);
        final InitialLoadTaskPartition partition = new InitialLoadTaskPartition("key", state);
        assertThat(partition.getProgressState().isPresent(), equalTo(true));
        assertThat(partition.getProgressState().get().getSnapshotId(), equalTo(200L));
        assertThat(partition.getProgressState().get().getTableName(), equalTo("db.table1"));
        assertThat(partition.getProgressState().get().getDataFilePath(), equalTo("/path/file.parquet"));
        assertThat(partition.getProgressState().get().getTotalRecords(), equalTo(1000L));
    }

    @Test
    void getProgressState_fromStoreItem_returnsRestoredState() {
        final SourcePartitionStoreItem item = mock(SourcePartitionStoreItem.class);
        when(item.getSourceIdentifier()).thenReturn("prefix|INITIAL_LOAD_TASK");
        when(item.getSourcePartitionKey()).thenReturn("db.table1|initial|uuid");
        when(item.getPartitionProgressState()).thenReturn(
                "{\"snapshot_id\":200,\"table_name\":\"db.table1\",\"data_file_path\":\"/path/file.parquet\",\"total_records\":1000}");

        final InitialLoadTaskPartition partition = new InitialLoadTaskPartition(item);
        assertThat(partition.getPartitionKey(), equalTo("db.table1|initial|uuid"));
        assertThat(partition.getProgressState().isPresent(), equalTo(true));

        final InitialLoadTaskProgressState state = partition.getProgressState().get();
        assertThat(state.getSnapshotId(), equalTo(200L));
        assertThat(state.getTableName(), equalTo("db.table1"));
        assertThat(state.getDataFilePath(), equalTo("/path/file.parquet"));
        assertThat(state.getTotalRecords(), equalTo(1000L));
    }
}
