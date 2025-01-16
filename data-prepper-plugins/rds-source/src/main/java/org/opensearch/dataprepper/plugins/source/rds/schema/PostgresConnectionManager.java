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

import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PostgresConnectionManager implements ConnectionManager {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresConnectionManager.class);

    public static final String JDBC_URL_FORMAT = "jdbc:postgresql://%s:%d/%s";
    public static final String SERVER_VERSION_9_4 = "9.4";
    public static final String DATABASE_REPLICATION = "database";
    public static final String SIMPLE_QUERY = "simple";
    public static final String TRUE_VALUE = "true";
    public static final String FALSE_VALUE = "false";
    public static final String REQUIRE_SSL = "require";

    private final String endpoint;
    private final int port;
    private final String username;
    private final String password;
    private final boolean requireSSL;
    private final String database;

    public PostgresConnectionManager(String endpoint, int port, String username, String password, boolean requireSSL, String database) {
        this.endpoint = endpoint;
        this.port = port;
        this.username = username;
        this.password = password;
        this.requireSSL = requireSSL;
        this.database = database;
    }

    @Override
    public Connection getConnection() throws SQLException {
        final Properties props = new Properties();
        PGProperty.USER.set(props, username);
        if (!password.isEmpty()) {
            PGProperty.PASSWORD.set(props, password);
        }
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, SERVER_VERSION_9_4);  // This is required
        PGProperty.REPLICATION.set(props, DATABASE_REPLICATION);   // This is also required
        PGProperty.PREFER_QUERY_MODE.set(props, SIMPLE_QUERY);

        if (requireSSL) {
            PGProperty.SSL.set(props, TRUE_VALUE);
            PGProperty.SSL_MODE.set(props, REQUIRE_SSL);
        } else {
            PGProperty.SSL.set(props, FALSE_VALUE);
        }

        final String jdbcUrl = String.format(JDBC_URL_FORMAT, this.endpoint, this.port, this.database);
        LOG.debug("Connecting to JDBC URL: {}", jdbcUrl);
        return doGetConnection(jdbcUrl, props);
    }

    // VisibleForTesting
    Connection doGetConnection(String jdbcUrl, Properties props) throws SQLException {
        return DriverManager.getConnection(jdbcUrl, props);
    }
}
