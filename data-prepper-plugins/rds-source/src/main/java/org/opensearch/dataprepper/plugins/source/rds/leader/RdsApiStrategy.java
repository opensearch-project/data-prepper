/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.leader;

import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import org.opensearch.dataprepper.plugins.source.rds.model.SnapshotInfo;

/**
 * Provides a strategy for creating and describing RDS snapshots.
 */
public interface RdsApiStrategy {
    /**
     * Describes an RDS instance or cluster.
     * @param dbIdentifier The identifier of the RDS instance or cluster to describe
     * @return An {@link DbMetadata} object describing the instance or cluster
     */
    DbMetadata describeDb(String dbIdentifier);

    /**
     * Creates a snapshot of an RDS instance or cluster.
     *
     * @param dbIdentifier The identifier of the RDS instance or cluster to snapshot.
     * @param snapshotId   The identifier of the snapshot.
     * @return An {@link SnapshotInfo} object describing the snapshot.
     */
    SnapshotInfo createSnapshot(String dbIdentifier, String snapshotId);

    /**
     * Checks the status of a snapshot.
     *
     * @param snapshotId   The identifier of the snapshot.
     * @return An {@link SnapshotInfo} object describing the snapshot.
     */
    SnapshotInfo describeSnapshot(String snapshotId);
}
