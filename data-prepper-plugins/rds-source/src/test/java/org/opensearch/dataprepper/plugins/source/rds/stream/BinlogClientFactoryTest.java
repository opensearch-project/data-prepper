/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BinlogClientFactoryTest {

    @Mock
    private RdsSourceConfig sourceConfig;

    @Mock
    private RdsClient rdsClient;

    private BinlogClientFactory binlogClientFactory;
    private Random random;

    @BeforeEach
    void setUp() {
        binlogClientFactory = createBinlogClientFactory();
        random = new Random();
    }

    @Test
    void test_create() {
        DescribeDbInstancesResponse describeDbInstancesResponse = mock(DescribeDbInstancesResponse.class);
        DBInstance dbInstance = mock(DBInstance.class, RETURNS_DEEP_STUBS);
        final String address = UUID.randomUUID().toString();
        final Integer port = random.nextInt();
        when(dbInstance.endpoint().address()).thenReturn(address);
        when(dbInstance.endpoint().port()).thenReturn(port);
        when(describeDbInstancesResponse.dbInstances()).thenReturn(List.of(dbInstance));
        when(sourceConfig.getDbIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(rdsClient.describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstancesResponse);
        RdsSourceConfig.AuthenticationConfig authenticationConfig = mock(RdsSourceConfig.AuthenticationConfig.class);
        when(sourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);

        binlogClientFactory.create();
    }

    private BinlogClientFactory createBinlogClientFactory() {
        return new BinlogClientFactory(sourceConfig, rdsClient);
    }
}