/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.opensearch.dataprepper.plugins.source.rds.model.SnapshotInfo;

import java.util.UUID;

public class SnapshotManager {
    private final SnapshotStrategy snapshotStrategy;

    public SnapshotManager(final SnapshotStrategy snapshotStrategy) {
        this.snapshotStrategy = snapshotStrategy;
    }

    public SnapshotInfo createSnapshot(String dbIdentifier) {
        final String snapshotId = generateSnapshotId(dbIdentifier);
        return snapshotStrategy.createSnapshot(dbIdentifier, snapshotId);
    }

    public SnapshotInfo checkSnapshotStatus(String snapshotId) {
        return snapshotStrategy.describeSnapshot(snapshotId);
    }

    private String generateSnapshotId(String dbIdentifier) {
        return dbIdentifier + "-snapshot-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
