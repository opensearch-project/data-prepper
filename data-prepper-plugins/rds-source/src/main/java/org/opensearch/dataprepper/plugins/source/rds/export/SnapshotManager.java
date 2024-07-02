/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.opensearch.dataprepper.plugins.source.rds.model.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;

import java.time.Instant;
import java.util.UUID;

public class SnapshotManager {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

    private final RdsClient rdsClient;

    public SnapshotManager(final RdsClient rdsClient) {
        this.rdsClient = rdsClient;
    }

    public SnapshotInfo createSnapshot(String dbInstanceId) {
        final String snapshotId = generateSnapshotId(dbInstanceId);
        CreateDbSnapshotRequest request = CreateDbSnapshotRequest.builder()
                .dbInstanceIdentifier(dbInstanceId)
                .dbSnapshotIdentifier(snapshotId)
                .build();

        try {
            CreateDbSnapshotResponse response = rdsClient.createDBSnapshot(request);
            String snapshotArn = response.dbSnapshot().dbSnapshotArn();
            String status = response.dbSnapshot().status();
            Instant createTime = response.dbSnapshot().snapshotCreateTime();
            LOG.info("Creating snapshot with id {} and status {}", snapshotId, status);

            return new SnapshotInfo(snapshotId, snapshotArn, createTime, status);
        } catch (Exception e) {
            LOG.error("Failed to create snapshot for {}", dbInstanceId, e);
            return null;
        }
    }

    public SnapshotInfo checkSnapshotStatus(String snapshotId) {
        DescribeDbSnapshotsRequest request = DescribeDbSnapshotsRequest.builder()
                .dbSnapshotIdentifier(snapshotId)
                .build();

        DescribeDbSnapshotsResponse response = rdsClient.describeDBSnapshots(request);
        String snapshotArn = response.dbSnapshots().get(0).dbSnapshotArn();
        String status = response.dbSnapshots().get(0).status();
        Instant createTime = response.dbSnapshots().get(0).snapshotCreateTime();

        return new SnapshotInfo(snapshotId, snapshotArn, createTime, status);
    }

    private String generateSnapshotId(String dbClusterId) {
        return dbClusterId + "-snapshot-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
