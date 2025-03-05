/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MySqlConnectionManager implements ConnectionManager {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(MySqlConnectionManager.class);

    static final String JDBC_URL_FORMAT = "jdbc:mysql://%s:%d";
    static final String USERNAME_KEY = "user";
    static final String PASSWORD_KEY = "password";
    static final String USE_SSL_KEY = "useSSL";
    static final String REQUIRE_SSL_KEY = "requireSSL";
    static final String TINY_INT_ONE_IS_BIT_KEY = "tinyInt1isBit";
    static final String TRUE_VALUE = "true";
    static final String FALSE_VALUE = "false";
    private final String hostName;
    private final int port;
    private final String username;
    private final String password;
    private final boolean requireSSL;

    public MySqlConnectionManager(String hostName, int port, String username, String password, boolean requireSSL) {
        this.hostName = hostName;
        this.port = port;
        this.username = username;
        this.password = password;
        this.requireSSL = requireSSL;
    }

    @Override
    public Connection getConnection() throws SQLException {
        final Properties props = new Properties();
        props.setProperty(USERNAME_KEY, username);
        props.setProperty(PASSWORD_KEY, password);
        if (requireSSL) {
            props.setProperty(USE_SSL_KEY, TRUE_VALUE);
            props.setProperty(REQUIRE_SSL_KEY, TRUE_VALUE);
        } else {
            props.setProperty(USE_SSL_KEY, FALSE_VALUE);
        }
        props.setProperty(TINY_INT_ONE_IS_BIT_KEY, FALSE_VALUE);
        final String jdbcUrl = String.format(JDBC_URL_FORMAT, hostName, port);
        LOG.debug("Connecting to JDBC URL: {}", jdbcUrl);
        return doGetConnection(jdbcUrl, props);
    }

    // VisibleForTesting
    Connection doGetConnection(String jdbcUrl, Properties props) throws SQLException {
        return DriverManager.getConnection(jdbcUrl, props);
    }
}
