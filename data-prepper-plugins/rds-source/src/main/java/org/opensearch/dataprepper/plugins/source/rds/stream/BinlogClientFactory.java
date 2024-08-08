/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import software.amazon.awssdk.services.rds.RdsClient;

public class BinlogClientFactory {

    private final RdsSourceConfig sourceConfig;
    private final RdsClient rdsClient;
    private final DbMetadata dbMetadata;

    public BinlogClientFactory(final RdsSourceConfig sourceConfig,
                               final RdsClient rdsClient,
                               final DbMetadata dbMetadata) {
        this.sourceConfig = sourceConfig;
        this.rdsClient = rdsClient;
        this.dbMetadata = dbMetadata;
    }

    public BinaryLogClient create() {
        return new BinaryLogClient(
                dbMetadata.getHostName(),
                dbMetadata.getPort(),
                sourceConfig.getAuthenticationConfig().getUsername(),
                sourceConfig.getAuthenticationConfig().getPassword());
    }
}
