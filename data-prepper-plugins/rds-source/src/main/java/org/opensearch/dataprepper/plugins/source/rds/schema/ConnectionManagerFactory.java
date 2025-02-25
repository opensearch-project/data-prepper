/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;

import java.util.List;

public class ConnectionManagerFactory {
    private final RdsSourceConfig sourceConfig;
    private final DbMetadata dbMetadata;

    public ConnectionManagerFactory(final RdsSourceConfig sourceConfig, final DbMetadata dbMetadata) {
        this.sourceConfig = sourceConfig;
        this.dbMetadata = dbMetadata;
    }

    public ConnectionManager getConnectionManager() {
        if (sourceConfig.getEngine().isMySql()) {
            return new MySqlConnectionManager(
                    dbMetadata.getEndpoint(),
                    dbMetadata.getPort(),
                    sourceConfig.getAuthenticationConfig().getUsername(),
                    sourceConfig.getAuthenticationConfig().getPassword(),
                    sourceConfig.isTlsEnabled());
        }

        return new PostgresConnectionManager(
                dbMetadata.getEndpoint(),
                dbMetadata.getPort(),
                sourceConfig.getAuthenticationConfig().getUsername(),
                sourceConfig.getAuthenticationConfig().getPassword(),
                sourceConfig.isTlsEnabled(),
                getDatabaseName(sourceConfig.getTableNames()));
    }

    private String getDatabaseName(List<String> tableNames) {
        return tableNames.get(0).split("\\.")[0];
    }
}
