/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg;

import io.micrometer.core.instrument.Counter;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.WriteResult;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.partition.WriteResultPartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.state.WriteResultState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runs on a single leader node, elected via lease-based coordination.
 * At each commit_interval, collects pending WriteResultPartitions from the coordination store,
 * reads the delta manifest files from storage to reconstruct WriteResults,
 * and commits them to Iceberg.
 * <p>
 * Failover: When a new leader takes over, it recovers the last commitSequence from
 * Iceberg snapshot properties and skips already-committed partitions.
 */
public class CommitScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CommitScheduler.class);
    private static final Duration LEASE_DURATION = Duration.ofMinutes(3);
    static final String COMMIT_SEQUENCE_PROPERTY = "data-prepper.commit-sequence";

    private final EnhancedSourceCoordinator coordinator;
    private final Catalog catalog;
    private final Duration commitInterval;
    private final Counter commitCount;
    private final ConcurrentHashMap<String, TableCommitState> tableStates;

    private LeaderPartition leaderPartition;

    private volatile boolean shutdownRequested;

    public CommitScheduler(final EnhancedSourceCoordinator coordinator,
                           final Catalog catalog,
                           final Duration commitInterval,
                           final Counter commitCount) {
        this.coordinator = coordinator;
        this.catalog = catalog;
        this.commitInterval = commitInterval;
        this.commitCount = commitCount;
        this.tableStates = new ConcurrentHashMap<>();
    }

    /**
     * Requests graceful shutdown. The CommitScheduler will execute one final commit cycle
     * to flush any pending WriteResultPartitions, then exit.
     */
    public void requestShutdown() {
        shutdownRequested = true;
    }

    @Override
    public void run() {
        LOG.info("Starting CommitScheduler");

        while (!shutdownRequested) {
            try {
                // Attempt to become leader if not already
                if (leaderPartition == null) {
                    final Optional<EnhancedSourcePartition> partition =
                            coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE);
                    if (partition.isPresent()) {
                        leaderPartition = (LeaderPartition) partition.get();
                        LOG.info("Acquired leader partition");
                    }
                }

                if (leaderPartition != null) {
                    commitPendingResults();
                    extendLease();
                }

                Thread.sleep(commitInterval.toMillis());
            } catch (final InterruptedException e) {
                if (shutdownRequested) {
                    LOG.info("CommitScheduler interrupted for shutdown, executing final commit");
                    break;
                }
                LOG.info("CommitScheduler interrupted");
                break;
            } catch (final Exception e) {
                LOG.error("Error in commit cycle", e);
                if (leaderPartition != null) {
                    try {
                        extendLease();
                    } catch (final Exception ex) {
                        LOG.error("Failed to extend lease, releasing leader partition");
                        leaderPartition = null;
                    }
                }
            }
        }

        // Final commit cycle to flush any remaining WriteResultPartitions from shutdown
        if (leaderPartition != null) {
            try {
                commitPendingResults();
                LOG.info("Final commit cycle completed");
            } catch (final Exception e) {
                LOG.error("Failed to execute final commit during shutdown", e);
            }
            coordinator.giveUpPartition(leaderPartition);
        }

        LOG.info("CommitScheduler stopped");
    }

    private void commitPendingResults() throws Exception {
        final List<WriteResultPartition> pending = collectPendingPartitions();
        if (pending.isEmpty()) {
            return;
        }

        // Dynamic routing may produce WriteResults for different tables in the same cycle.
        // Group by table so each table gets an independent Iceberg commit.
        final Map<String, List<WriteResultPartition>> byTable = new HashMap<>();
        for (final WriteResultPartition partition : pending) {
            final WriteResultState state = partition.getProgressState().orElseThrow();
            byTable.computeIfAbsent(state.getTableIdentifier(), k -> new ArrayList<>()).add(partition);
        }

        for (final Map.Entry<String, List<WriteResultPartition>> entry : byTable.entrySet()) {
            commitForTable(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Commits WriteResults for a single table. The flow is:
     * 1. Initialize table state on first access (load table, recover commitSequence from snapshots)
     * 2. Skip partitions that were already committed (detected by commitSequence comparison)
     * 3. Assign the next commitSequence and persist it to each WriteResultPartition before committing
     * 4. Read delta manifest files from S3 to reconstruct WriteResults
     * 5. Commit to Iceberg:
     *    - Append-only: single newAppend() for all WriteResults
     *    - CDC (has delete files): individual newRowDelta() per WriteResult, because mixing
     *      delete files from different WriteResults in one RowDelta can break sequence number semantics
     * 6. Clean up: delete delta manifest files from S3 and complete partitions in coordination store
     */
    private void commitForTable(final String tableIdentifier,
                                final List<WriteResultPartition> partitions) throws Exception {
        // Load table and recover commitSequence on first access for this table.
        final TableCommitState tableState = tableStates.computeIfAbsent(tableIdentifier, id -> {
            final Table table = catalog.loadTable(TableIdentifier.parse(id));
            final long seq = getMaxCommittedSequence(table);
            LOG.info("Initialized commit state for table {}, resuming from commitSequence={}", id, seq);
            return new TableCommitState(table, new DeltaManifestWriter(table), seq);
        });

        final Table table = tableState.table;
        final DeltaManifestWriter deltaManifestWriter = tableState.deltaManifestWriter;

        // After failover, some partitions may have commitSequence from the previous leader.
        // If that sequence was already committed to Iceberg, skip them.
        final List<WriteResultPartition> toCommit = new ArrayList<>();
        for (final WriteResultPartition partition : partitions) {
            final WriteResultState state = partition.getProgressState().orElseThrow();
            if (state.getCommitSequence() != null && state.getCommitSequence() <= tableState.commitSequence) {
                LOG.info("Partition already committed (sequence={}), completing", state.getCommitSequence());
                deltaManifestWriter.delete(state);
                coordinator.completePartition(partition);
            } else {
                toCommit.add(partition);
            }
        }

        if (toCommit.isEmpty()) {
            return;
        }

        // Assign commitSequence and persist to coordination store BEFORE committing to Iceberg.
        // This allows a new leader to determine whether the commit succeeded by checking
        // if a snapshot with this commitSequence exists.
        tableState.commitSequence++;
        final long commitSequence = tableState.commitSequence;

        for (final WriteResultPartition partition : toCommit) {
            final WriteResultState state = partition.getProgressState().orElseThrow();
            state.setCommitSequence(commitSequence);
            coordinator.saveProgressStateForPartition(partition, LEASE_DURATION);
        }

        LOG.info("Committing {} write results for table {} with sequence={}",
                toCommit.size(), tableIdentifier, commitSequence);

        // Read delta manifest files from S3 to reconstruct WriteResults
        final List<WriteResult> results = new ArrayList<>();
        boolean hasDeleteFiles = false;
        for (final WriteResultPartition partition : toCommit) {
            final WriteResultState state = partition.getProgressState().orElseThrow();
            final WriteResult result = deltaManifestWriter.read(state);
            results.add(result);
            if (result.deleteFiles().length > 0) {
                hasDeleteFiles = true;
            }
        }

        if (!hasDeleteFiles) {
            // Append-only: all WriteResults can be combined into a single Iceberg append.
            final AppendFiles append = table.newAppend();
            for (final WriteResult result : results) {
                Arrays.stream(result.dataFiles()).forEach(append::appendFile);
            }
            append.set(COMMIT_SEQUENCE_PROPERTY, String.valueOf(commitSequence));
            append.commit();
        } else {
            // CDC: each WriteResult is committed as a separate RowDelta because equality delete
            // files have sequence number semantics that require individual commits.
            // Each sub-commit is tagged with "sequence-subIndex" (e.g., "5-0", "5-1") so that
            // failover recovery can determine which sub-commits completed.
            table.refresh();
            for (int i = 0; i < results.size(); i++) {
                final WriteResultState state = toCommit.get(i).getProgressState().orElseThrow();
                state.setSubIndex(i);
                coordinator.saveProgressStateForPartition(toCommit.get(i), LEASE_DURATION);

                final String seqValue = commitSequence + "-" + i;
                if (isSubCommitDone(table, commitSequence, seqValue)) {
                    continue;
                }

                final WriteResult result = results.get(i);
                final RowDelta rowDelta = table.newRowDelta();
                Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
                Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
                rowDelta.set(COMMIT_SEQUENCE_PROPERTY, seqValue);
                rowDelta.commit();
                table.refresh();
            }
        }

        // Clean up: delete delta manifest files from S3 and mark partitions as complete
        for (final WriteResultPartition partition : toCommit) {
            deltaManifestWriter.delete(partition.getProgressState().orElseThrow());
            coordinator.completePartition(partition);
        }

        LOG.info("Committed sequence={} for table {}", commitSequence, tableIdentifier);
        commitCount.increment();
    }

    /**
     * Checks if a specific sub-commit (e.g., "5-2") was already committed by a previous leader.
     * Walks snapshot history backwards, stopping when it reaches a snapshot from a different
     * commitSequence.
     */
    private boolean isSubCommitDone(final Table table, final long commitSequence, final String seqValue) {
        Snapshot snapshot = table.currentSnapshot();
        while (snapshot != null) {
            final String value = snapshot.summary().get(COMMIT_SEQUENCE_PROPERTY);
            if (seqValue.equals(value)) {
                return true;
            }
            if (value != null && !value.startsWith(String.valueOf(commitSequence))) {
                break;
            }
            final Long parentId = snapshot.parentId();
            snapshot = parentId != null ? table.snapshot(parentId) : null;
        }
        return false;
    }

    /**
     * Recovers the highest commitSequence from Iceberg snapshot history.
     * Walks backwards from the current snapshot looking for the data-prepper.commit-sequence
     * property. Snapshots created by other engines (Spark, etc.) are skipped.
     * Returns 0 if no Data Prepper commits have been made to this table.
     */
    static long getMaxCommittedSequence(final Table table) {
        table.refresh();
        Snapshot snapshot = table.currentSnapshot();
        while (snapshot != null) {
            final Map<String, String> summary = snapshot.summary();
            final String value = summary.get(COMMIT_SEQUENCE_PROPERTY);
            if (value != null) {
                final String seqPart = value.contains("-") ? value.substring(0, value.indexOf('-')) : value;
                return Long.parseLong(seqPart);
            }
            final Long parentId = snapshot.parentId();
            snapshot = parentId != null ? table.snapshot(parentId) : null;
        }
        return 0L;
    }

    private List<WriteResultPartition> collectPendingPartitions() {
        final List<WriteResultPartition> partitions = new ArrayList<>();
        while (true) {
            final Optional<EnhancedSourcePartition> partition =
                    coordinator.acquireAvailablePartition(WriteResultPartition.PARTITION_TYPE);
            if (partition.isEmpty()) {
                break;
            }
            partitions.add((WriteResultPartition) partition.get());
        }
        return partitions;
    }

    private void extendLease() {
        coordinator.saveProgressStateForPartition(leaderPartition, LEASE_DURATION);
    }

    private static class TableCommitState {
        final Table table;
        final DeltaManifestWriter deltaManifestWriter;
        long commitSequence;

        TableCommitState(final Table table, final DeltaManifestWriter deltaManifestWriter, final long commitSequence) {
            this.table = table;
            this.deltaManifestWriter = deltaManifestWriter;
            this.commitSequence = commitSequence;
        }
    }
}
