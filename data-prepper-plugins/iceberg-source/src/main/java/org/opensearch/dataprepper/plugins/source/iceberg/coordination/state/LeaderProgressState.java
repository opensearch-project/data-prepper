/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LeaderProgressState {

    @JsonProperty("initialized")
    private boolean initialized;

    @JsonProperty("last_processed_snapshot_id")
    private Long lastProcessedSnapshotId;

    public boolean isInitialized() {
        return initialized;
    }

    public void setInitialized(final boolean initialized) {
        this.initialized = initialized;
    }

    public Long getLastProcessedSnapshotId() {
        return lastProcessedSnapshotId;
    }

    public void setLastProcessedSnapshotId(final Long lastProcessedSnapshotId) {
        this.lastProcessedSnapshotId = lastProcessedSnapshotId;
    }
}
