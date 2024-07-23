/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
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
        DBInstance dbInstance = describeDbInstance(sourceConfig.getDbIdentifier());
        return new BinaryLogClient(
                dbInstance.endpoint().address(),
                dbInstance.endpoint().port(),
                // For test
                // "127.0.0.1",
                // 3306,
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
}
