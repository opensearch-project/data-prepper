/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.schema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionManager {
    static final String JDBC_URL_FORMAT = "jdbc:mysql://%s:%d";
    private final String hostName;
    private final int port;
    private final String username;
    private final String password;
    private final boolean requireSSL;

    public ConnectionManager(String hostName, int port, String username, String password, boolean requireSSL) {
        this.hostName = hostName;
        this.port = port;
        this.username = username;
        this.password = password;
        this.requireSSL = requireSSL;
    }

    public Connection getConnection() throws SQLException {
        final Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        if (requireSSL) {
            props.setProperty("useSSL", "true");
            props.setProperty("requireSSL", "true");
        } else {
            props.setProperty("useSSL", "false");
        }
        final String jdbcUrl = String.format(JDBC_URL_FORMAT, hostName, port);
        return doGetConnection(jdbcUrl, props);
    }

    // VisibleForTesting
    Connection doGetConnection(String jdbcUrl, Properties props) throws SQLException {
        return DriverManager.getConnection(jdbcUrl, props);
    }
}
