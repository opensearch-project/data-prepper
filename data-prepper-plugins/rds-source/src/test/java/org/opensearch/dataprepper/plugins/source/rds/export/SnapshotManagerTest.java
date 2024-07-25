/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SnapshotManagerTest {

    @Mock
    private SnapshotStrategy snapshotStrategy;

    private SnapshotManager snapshotManager;

    @BeforeEach
    void setUp() {
        snapshotManager = createObjectUnderTest();
    }

    @Test
    void test_create_snapshot() {
        final String dbIdentifier = UUID.randomUUID().toString();

        snapshotManager.createSnapshot(dbIdentifier);

        verify(snapshotStrategy).createSnapshot(eq(dbIdentifier), startsWith(dbIdentifier + "-snapshot-"));
    }

    @Test
    void test_check_snapshot_status() {
        final String snapshotId = UUID.randomUUID().toString();

        snapshotManager.checkSnapshotStatus(snapshotId);

        verify(snapshotStrategy).describeSnapshot(snapshotId);
    }

    private SnapshotManager createObjectUnderTest() {
        return new SnapshotManager(snapshotStrategy);
    }
}