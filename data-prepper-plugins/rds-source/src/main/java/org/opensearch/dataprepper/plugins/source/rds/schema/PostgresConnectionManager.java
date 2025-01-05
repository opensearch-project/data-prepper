package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PostgresConnectionManager {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresConnectionManager.class);

    static final String URL_FORMAT = "jdbc:postgresql://%s:%d/%s";

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

    public Connection getConnection() throws SQLException {
        final Properties props = new Properties();
        PGProperty.USER.set(props, username);
        if (!password.isEmpty()) {
            PGProperty.PASSWORD.set(props, password);
        }
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");  // This is required
        PGProperty.REPLICATION.set(props, "database");   // This is also required
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        if (requireSSL) {
            PGProperty.SSL.set(props, "true");
            PGProperty.SSL_MODE.set(props, "require");
        } else {
            PGProperty.SSL.set(props, "false");
        }

        final String jdbcUrl = String.format(URL_FORMAT, this.endpoint, this.port, this.database);
        LOG.debug("Connecting to JDBC URL: {}", jdbcUrl);
        return doGetConnection(jdbcUrl, props);
    }

    // VisibleForTesting
    Connection doGetConnection(String jdbcUrl, Properties props) throws SQLException {
        return DriverManager.getConnection(jdbcUrl, props);
    }
}
