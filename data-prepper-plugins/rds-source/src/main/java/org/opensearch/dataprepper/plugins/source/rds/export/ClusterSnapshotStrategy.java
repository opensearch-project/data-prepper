/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.opensearch.dataprepper.plugins.source.rds.model.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsResponse;

import java.time.Instant;

/**
 * This snapshot strategy works with RDS/Aurora Clusters
 */
public class ClusterSnapshotStrategy implements SnapshotStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterSnapshotStrategy.class);
    private final RdsClient rdsClient;

    public ClusterSnapshotStrategy(final RdsClient rdsClient) {
        this.rdsClient = rdsClient;
    }

    @Override
    public SnapshotInfo createSnapshot(String dbIdentifier, String snapshotId) {
        CreateDbClusterSnapshotRequest request = CreateDbClusterSnapshotRequest.builder()
                .dbClusterIdentifier(dbIdentifier)
                .dbClusterSnapshotIdentifier(snapshotId)
                .build();

        try {
            CreateDbClusterSnapshotResponse response = rdsClient.createDBClusterSnapshot(request);
            String snapshotArn = response.dbClusterSnapshot().dbClusterSnapshotArn();
            String status = response.dbClusterSnapshot().status();
            Instant createTime = response.dbClusterSnapshot().snapshotCreateTime();
            LOG.info("Creating snapshot with id {} for {} and the current status is {}", snapshotId, dbIdentifier, status);

            return new SnapshotInfo(snapshotId, snapshotArn, createTime, status);
        } catch (Exception e) {
            LOG.error("Failed to create snapshot for {}", dbIdentifier, e);
            return null;
        }
    }

    @Override
    public SnapshotInfo describeSnapshot(String snapshotId) {
        DescribeDbClusterSnapshotsRequest request = DescribeDbClusterSnapshotsRequest.builder()
                .dbClusterSnapshotIdentifier(snapshotId)
                .build();

        DescribeDbClusterSnapshotsResponse response = rdsClient.describeDBClusterSnapshots(request);
        String snapshotArn = response.dbClusterSnapshots().get(0).dbClusterSnapshotArn();
        String status = response.dbClusterSnapshots().get(0).status();
        Instant createTime = response.dbClusterSnapshots().get(0).snapshotCreateTime();

        return new SnapshotInfo(snapshotId, snapshotArn, createTime, status);
    }
}
