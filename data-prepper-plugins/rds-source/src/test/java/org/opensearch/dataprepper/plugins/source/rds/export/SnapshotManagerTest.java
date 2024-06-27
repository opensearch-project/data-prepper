/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.model.SnapshotInfo;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SnapshotManagerTest {

    @Mock
    private RdsClient rdsClient;

    private SnapshotManager snapshotManager;

    @BeforeEach
    void setUp() {
        snapshotManager = createObjectUnderTest();
    }

    @Test
    void test_create_snapshot_with_success() {
        String dbInstanceId = UUID.randomUUID().toString();
        CreateDbSnapshotResponse createDbSnapshotResponse = mock(CreateDbSnapshotResponse.class);
        DBSnapshot dbSnapshot = mock(DBSnapshot.class);
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:snapshot-0b5ae174";
        final String status = "creating";
        final Instant createTime = Instant.now();
        when(dbSnapshot.dbSnapshotArn()).thenReturn(snapshotArn);
        when(dbSnapshot.status()).thenReturn(status);
        when(dbSnapshot.snapshotCreateTime()).thenReturn(createTime);
        when(createDbSnapshotResponse.dbSnapshot()).thenReturn(dbSnapshot);
        when(rdsClient.createDBSnapshot(any(CreateDbSnapshotRequest.class))).thenReturn(createDbSnapshotResponse);

        SnapshotInfo snapshotInfo = snapshotManager.createSnapshot(dbInstanceId);

        ArgumentCaptor<CreateDbSnapshotRequest> argumentCaptor = ArgumentCaptor.forClass(CreateDbSnapshotRequest.class);
        verify(rdsClient).createDBSnapshot(argumentCaptor.capture());

        CreateDbSnapshotRequest request = argumentCaptor.getValue();
        assertThat(request.dbInstanceIdentifier(), equalTo(dbInstanceId));

        assertThat(snapshotInfo, notNullValue());
        assertThat(snapshotInfo.getSnapshotArn(), equalTo(snapshotArn));
        assertThat(snapshotInfo.getStatus(), equalTo(status));
        assertThat(snapshotInfo.getCreateTime(), equalTo(createTime));
    }

    @Test
    void test_create_snapshot_throws_exception_then_returns_null() {
        String dbInstanceId = UUID.randomUUID().toString();
        when(rdsClient.createDBSnapshot(any(CreateDbSnapshotRequest.class))).thenThrow(new RuntimeException("Error"));

        SnapshotInfo snapshotInfo = snapshotManager.createSnapshot(dbInstanceId);

        assertThat(snapshotInfo, equalTo(null));
    }

    @Test
    void test_check_snapshot_status_returns_correct_result() {
        DBSnapshot dbSnapshot = mock(DBSnapshot.class);
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:snapshot-0b5ae174";
        final String status = "creating";
        final Instant createTime = Instant.now();
        when(dbSnapshot.dbSnapshotArn()).thenReturn(snapshotArn);
        when(dbSnapshot.status()).thenReturn(status);
        when(dbSnapshot.snapshotCreateTime()).thenReturn(createTime);
        DescribeDbSnapshotsResponse describeDbSnapshotsResponse = mock(DescribeDbSnapshotsResponse.class);
        when(describeDbSnapshotsResponse.dbSnapshots()).thenReturn(List.of(dbSnapshot));

        final String snapshotId = UUID.randomUUID().toString();
        DescribeDbSnapshotsRequest describeDbSnapshotsRequest = DescribeDbSnapshotsRequest.builder()
                .dbSnapshotIdentifier(snapshotId)
                .build();
        when(rdsClient.describeDBSnapshots(describeDbSnapshotsRequest)).thenReturn(describeDbSnapshotsResponse);

        SnapshotInfo snapshotInfo = snapshotManager.checkSnapshotStatus(snapshotId);

        assertThat(snapshotInfo, notNullValue());
        assertThat(snapshotInfo.getSnapshotId(), equalTo(snapshotId));
        assertThat(snapshotInfo.getSnapshotArn(), equalTo(snapshotArn));
        assertThat(snapshotInfo.getStatus(), equalTo(status));
        assertThat(snapshotInfo.getCreateTime(), equalTo(createTime));
    }

    private SnapshotManager createObjectUnderTest() {
        return new SnapshotManager(rdsClient);
    }
}