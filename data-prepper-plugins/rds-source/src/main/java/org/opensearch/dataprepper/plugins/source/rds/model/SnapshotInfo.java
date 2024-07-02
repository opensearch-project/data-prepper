/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.time.Instant;

public class SnapshotInfo {

    private final String snapshotId;
    private final String snapshotArn;
    private final Instant createTime;
    private String status;

    public SnapshotInfo(String snapshotId, String snapshotArn, Instant createTime, String status) {
        this.snapshotId = snapshotId;
        this.snapshotArn = snapshotArn;
        this.createTime = createTime;
        this.status = status;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getSnapshotArn() {
        return snapshotArn;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
