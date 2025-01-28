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

public class SchemaManagerFactory {
    private final ConnectionManager connectionManager;

    public SchemaManagerFactory(final ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public SchemaManager getSchemaManager() {
        if (connectionManager instanceof MySqlConnectionManager) {
            return new MySqlSchemaManager(connectionManager);
        }

        return new PostgresSchemaManager(connectionManager);
    }
}
