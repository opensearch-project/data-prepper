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
class InstanceSnapshotStrategyTest {

    @Mock
    private RdsClient rdsClient;

    private InstanceSnapshotStrategy objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = createObjectUnderTest();
    }

    @Test
    void test_create_snapshot_with_success() {
        final String dbInstanceId = UUID.randomUUID().toString();
        final String snapshotId = UUID.randomUUID().toString();
        CreateDbSnapshotResponse createDbSnapshotResponse = mock(CreateDbSnapshotResponse.class);
        DBSnapshot dbSnapshot = mock(DBSnapshot.class);
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:" + snapshotId;
        final String status = "creating";
        final Instant createTime = Instant.now();
        when(dbSnapshot.dbSnapshotArn()).thenReturn(snapshotArn);
        when(dbSnapshot.status()).thenReturn(status);
        when(dbSnapshot.snapshotCreateTime()).thenReturn(createTime);
        when(createDbSnapshotResponse.dbSnapshot()).thenReturn(dbSnapshot);
        when(rdsClient.createDBSnapshot(any(CreateDbSnapshotRequest.class))).thenReturn(createDbSnapshotResponse);

        SnapshotInfo snapshotInfo = objectUnderTest.createSnapshot(dbInstanceId, snapshotId);

        ArgumentCaptor<CreateDbSnapshotRequest> argumentCaptor = ArgumentCaptor.forClass(CreateDbSnapshotRequest.class);
        verify(rdsClient).createDBSnapshot(argumentCaptor.capture());

        CreateDbSnapshotRequest request = argumentCaptor.getValue();
        assertThat(request.dbInstanceIdentifier(), equalTo(dbInstanceId));
        assertThat(request.dbSnapshotIdentifier(), equalTo(snapshotId));

        assertThat(snapshotInfo, notNullValue());
        assertThat(snapshotInfo.getSnapshotArn(), equalTo(snapshotArn));
        assertThat(snapshotInfo.getStatus(), equalTo(status));
        assertThat(snapshotInfo.getCreateTime(), equalTo(createTime));
    }

    @Test
    void test_create_snapshot_throws_exception_then_returns_null() {
        final String dbInstanceId = UUID.randomUUID().toString();
        final String snapshotId = UUID.randomUUID().toString();
        when(rdsClient.createDBSnapshot(any(CreateDbSnapshotRequest.class))).thenThrow(new RuntimeException("Error"));

        SnapshotInfo snapshotInfo = objectUnderTest.createSnapshot(dbInstanceId, snapshotId);

        assertThat(snapshotInfo, equalTo(null));
    }

    @Test
    void test_check_snapshot_status_returns_correct_result() {
        DBSnapshot dbSnapshot = mock(DBSnapshot.class);
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:" + UUID.randomUUID();
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

        SnapshotInfo snapshotInfo = objectUnderTest.describeSnapshot(snapshotId);

        assertThat(snapshotInfo, notNullValue());
        assertThat(snapshotInfo.getSnapshotId(), equalTo(snapshotId));
        assertThat(snapshotInfo.getSnapshotArn(), equalTo(snapshotArn));
        assertThat(snapshotInfo.getStatus(), equalTo(status));
        assertThat(snapshotInfo.getCreateTime(), equalTo(createTime));
    }

    private InstanceSnapshotStrategy createObjectUnderTest() {
        return new InstanceSnapshotStrategy(rdsClient);
    }
}