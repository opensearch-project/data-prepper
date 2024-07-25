/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;

public class BinlogClientFactory {

    private final RdsSourceConfig sourceConfig;

    private final RdsClient rdsClient;

    public BinlogClientFactory(final RdsSourceConfig sourceConfig, final RdsClient rdsClient) {
        this.sourceConfig = sourceConfig;
        this.rdsClient = rdsClient;
    }

    public BinaryLogClient create() {
        String hostName;
        int port;
        if (sourceConfig.isCluster()) {
            DBCluster dbCluster = describeDbCluster(sourceConfig.getDbIdentifier());
            hostName = dbCluster.endpoint();
            port = dbCluster.port();
        } else {
            DBInstance dbInstance = describeDbInstance(sourceConfig.getDbIdentifier());
            hostName = dbInstance.endpoint().address();
            port = dbInstance.endpoint().port();
        }
        return new BinaryLogClient(
                hostName,
                port,
                sourceConfig.getAuthenticationConfig().getUsername(),
                sourceConfig.getAuthenticationConfig().getPassword());
    }

    private DBInstance describeDbInstance(final String dbInstanceIdentifier) {
        DescribeDbInstancesRequest request = DescribeDbInstancesRequest.builder()
                .dbInstanceIdentifier(dbInstanceIdentifier)
                .build();

        DescribeDbInstancesResponse response = rdsClient.describeDBInstances(request);
        return response.dbInstances().get(0);
    }

    private DBCluster describeDbCluster(final String dbClusterIdentifier) {
        DescribeDbClustersRequest request = DescribeDbClustersRequest.builder()
                .dbClusterIdentifier(dbClusterIdentifier)
                .build();

        DescribeDbClustersResponse response = rdsClient.describeDBClusters(request);
        return response.dbClusters().get(0);
    }
}
