/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.leader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import org.opensearch.dataprepper.plugins.source.rds.model.SnapshotInfo;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterApiStrategyTest {

    @Mock
    private RdsClient rdsClient;

    private ClusterApiStrategy objectUnderTest;

    private final Random random = new Random();

    @BeforeEach
    void setUp() {
        objectUnderTest = createObjectUnderTest();
    }

    @Test
    void test_describeDb_returns_correct_results() {
        final String dbClusterId = UUID.randomUUID().toString();
        final String endpoint = UUID.randomUUID().toString();
        final int port = random.nextInt();
        final String readerEndpoint = UUID.randomUUID().toString();
        final DescribeDbClustersRequest describeDbClustersRequest = DescribeDbClustersRequest.builder()
                .dbClusterIdentifier(dbClusterId)
                .build();
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder()
                .dbClusters(DBCluster.builder()
                        .endpoint(endpoint)
                        .port(port)
                        .readerEndpoint(readerEndpoint)
                        .build())
                .build();
        when(rdsClient.describeDBClusters(describeDbClustersRequest)).thenReturn(describeDbClustersResponse);

        DbMetadata dbMetadata = objectUnderTest.describeDb(dbClusterId);

        assertThat(dbMetadata.getDbIdentifier(), equalTo(dbClusterId));
        assertThat(dbMetadata.getEndpoint(), equalTo(endpoint));
        assertThat(dbMetadata.getPort(), equalTo(port));
        assertThat(dbMetadata.getReaderEndpoint(), equalTo(readerEndpoint));
        assertThat(dbMetadata.getReaderPort(), equalTo(port));
    }

    @Test
    void test_create_snapshot_with_success() {
        final String dbInstanceId = UUID.randomUUID().toString();
        final String snapshotId = UUID.randomUUID().toString();
        CreateDbClusterSnapshotResponse createDbClusterSnapshotResponse = mock(CreateDbClusterSnapshotResponse.class);
        DBClusterSnapshot dbClusterSnapshot = mock(DBClusterSnapshot.class);
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:" + snapshotId;
        final String status = "creating";
        final Instant createTime = Instant.now();
        when(dbClusterSnapshot.dbClusterSnapshotArn()).thenReturn(snapshotArn);
        when(dbClusterSnapshot.status()).thenReturn(status);
        when(dbClusterSnapshot.snapshotCreateTime()).thenReturn(createTime);
        when(createDbClusterSnapshotResponse.dbClusterSnapshot()).thenReturn(dbClusterSnapshot);
        when(rdsClient.createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class))).thenReturn(createDbClusterSnapshotResponse);

        SnapshotInfo snapshotInfo = objectUnderTest.createSnapshot(dbInstanceId, snapshotId);

        ArgumentCaptor<CreateDbClusterSnapshotRequest> argumentCaptor = ArgumentCaptor.forClass(CreateDbClusterSnapshotRequest.class);
        verify(rdsClient).createDBClusterSnapshot(argumentCaptor.capture());

        CreateDbClusterSnapshotRequest request = argumentCaptor.getValue();
        assertThat(request.dbClusterIdentifier(), equalTo(dbInstanceId));
        assertThat(request.dbClusterSnapshotIdentifier(), equalTo(snapshotId));

        assertThat(snapshotInfo, notNullValue());
        assertThat(snapshotInfo.getSnapshotArn(), equalTo(snapshotArn));
        assertThat(snapshotInfo.getStatus(), equalTo(status));
        assertThat(snapshotInfo.getCreateTime(), equalTo(createTime));
    }

    @Test
    void test_create_snapshot_throws_exception_then_returns_null() {
        final String dbInstanceId = UUID.randomUUID().toString();
        final String snapshotId = UUID.randomUUID().toString();
        when(rdsClient.createDBClusterSnapshot(any(CreateDbClusterSnapshotRequest.class))).thenThrow(new RuntimeException("Error"));

        SnapshotInfo snapshotInfo = objectUnderTest.createSnapshot(dbInstanceId, snapshotId);

        assertThat(snapshotInfo, equalTo(null));
    }

    @Test
    void test_check_snapshot_status_returns_correct_result() {
        DBClusterSnapshot dbClusterSnapshot = mock(DBClusterSnapshot.class);
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:" + UUID.randomUUID();
        final String status = "creating";
        final Instant createTime = Instant.now();
        when(dbClusterSnapshot.dbClusterSnapshotArn()).thenReturn(snapshotArn);
        when(dbClusterSnapshot.status()).thenReturn(status);
        when(dbClusterSnapshot.snapshotCreateTime()).thenReturn(createTime);
        DescribeDbClusterSnapshotsResponse describeDbClusterSnapshotsResponse = mock(DescribeDbClusterSnapshotsResponse.class);
        when(describeDbClusterSnapshotsResponse.dbClusterSnapshots()).thenReturn(List.of(dbClusterSnapshot));

        final String snapshotId = UUID.randomUUID().toString();
        DescribeDbClusterSnapshotsRequest describeDbClusterSnapshotsRequest = DescribeDbClusterSnapshotsRequest.builder()
                .dbClusterSnapshotIdentifier(snapshotId)
                .build();
        when(rdsClient.describeDBClusterSnapshots(describeDbClusterSnapshotsRequest)).thenReturn(describeDbClusterSnapshotsResponse);

        SnapshotInfo snapshotInfo = objectUnderTest.describeSnapshot(snapshotId);

        assertThat(snapshotInfo, notNullValue());
        assertThat(snapshotInfo.getSnapshotId(), equalTo(snapshotId));
        assertThat(snapshotInfo.getSnapshotArn(), equalTo(snapshotArn));
        assertThat(snapshotInfo.getStatus(), equalTo(status));
        assertThat(snapshotInfo.getCreateTime(), equalTo(createTime));
    }

    private ClusterApiStrategy createObjectUnderTest() {
        return new ClusterApiStrategy(rdsClient);
    }
}