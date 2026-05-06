/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.leader;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.iceberg.TableConfig;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.InitialLoadTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.LeaderPartition;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaderSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;
    @Mock
    private Table table;
    @Mock
    private TableConfig tableConfig;

    private LeaderScheduler leaderScheduler;

    private static final String TABLE_NAME = "db.test_table";
    private static final long SNAPSHOT_ID = 123L;

    @BeforeEach
    void setUp() {
        leaderScheduler = new LeaderScheduler(
                sourceCoordinator,
                Map.of(TABLE_NAME, tableConfig),
                Duration.ofSeconds(1),
                Map.of(TABLE_NAME, table));
    }

    @Test
    void completionTrackingState_isCreated_beforeTaskPartitions_soWorkersCanReportCompletion() throws Exception {
        // Arrange: LeaderPartition with initialized=false
        final LeaderPartition leaderPartition = new LeaderPartition();
        when(sourceCoordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE))
                .thenReturn(Optional.of(leaderPartition));

        // Arrange: Table with one snapshot
        when(tableConfig.isDisableExport()).thenReturn(false);
        final Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.snapshotId()).thenReturn(SNAPSHOT_ID);
        when(table.currentSnapshot()).thenReturn(snapshot);

        // Arrange: TableScan returns one FileScanTask
        final TableScan tableScan = mock(TableScan.class);
        when(table.newScan()).thenReturn(tableScan);
        when(tableScan.useSnapshot(SNAPSHOT_ID)).thenReturn(tableScan);

        final FileScanTask fileScanTask = mock(FileScanTask.class);
        final DataFile dataFile = mock(DataFile.class);
        when(dataFile.location()).thenReturn("s3://bucket/data/file.parquet");
        when(dataFile.recordCount()).thenReturn(100L);
        when(fileScanTask.file()).thenReturn(dataFile);

        final CloseableIterable<FileScanTask> scanTasks = CloseableIterable.withNoopClose(List.of(fileScanTask));
        when(tableScan.planFiles()).thenReturn(scanTasks);

        // Arrange: waitForSnapshotComplete returns immediately (completed >= total)
        final String completionKey = "snapshot-completion-initial-" + SNAPSHOT_ID;
        final GlobalState completedState = new GlobalState(completionKey,
                Map.of("total", 1, "completed", 1));
        when(sourceCoordinator.getPartition(completionKey))
                .thenReturn(Optional.of(completedState));

        // Use a latch to know when performInitialLoad has finished
        final CountDownLatch initialLoadDone = new CountDownLatch(1);
        doAnswer(invocation -> {
            // saveProgressStateForPartition is called after waitForSnapshotComplete returns
            initialLoadDone.countDown();
            return null;
        }).when(sourceCoordinator).saveProgressStateForPartition(any(LeaderPartition.class), any(Duration.class));

        // Act: run in a thread
        final Thread thread = new Thread(leaderScheduler);
        thread.start();
        assertThat("performInitialLoad should complete within timeout",
                initialLoadDone.await(5, TimeUnit.SECONDS), is(true));
        thread.interrupt();
        thread.join(2000);

        // Assert: GlobalState (completion tracking) is created BEFORE InitialLoadTaskPartition
        final InOrder inOrder = inOrder(sourceCoordinator);
        inOrder.verify(sourceCoordinator).createPartition(argThat(
                (EnhancedSourcePartition<?> p) -> p instanceof GlobalState));
        inOrder.verify(sourceCoordinator).createPartition(argThat(
                (EnhancedSourcePartition<?> p) -> p instanceof InitialLoadTaskPartition));
    }
}
