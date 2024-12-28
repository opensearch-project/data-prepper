package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class LogicalReplicationClient implements ReplicationLogClient {

    private static final Logger LOG = LoggerFactory.getLogger(LogicalReplicationClient.class);
    private static final String URL_FORMAT = "jdbc:postgresql://%s:%d/%s";
    private final String endpoint;
    private final int port;
    private final String username;
    private final String password;
    private final String database;

    private final String jdbcUrl;
    private Properties props;

    private PostgresReplicationEventProcessor eventProcessor;

    public LogicalReplicationClient(final String endpoint,
                                    final int port,
                                    final String username,
                                    final String password,
                                    final String database) {
//        this.endpoint = endpoint;
//        this.port = port;
        this.endpoint = "127.0.0.1";
        this.port = 5432;
        this.username = username;
        this.password = password;
        this.database = database;
        jdbcUrl = String.format(URL_FORMAT, this.endpoint, this.port, this.database);
        props = new Properties();
    }

    public void setEventProcessor(PostgresReplicationEventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    public void connect() {
        PGProperty.USER.set(props, username);
        if (!password.isEmpty()) {
            PGProperty.PASSWORD.set(props, password);
        }
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");  // This is required
        PGProperty.REPLICATION.set(props, "database");   // This is also required
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");  // This is also required

        LOG.info("Connect to server with JDBC URL: {}", jdbcUrl);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
            // TODO: remove hard-coded tables
            final String createPublicationStatement = "CREATE PUBLICATION my_publication FOR TABLE cars, houses;";
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(createPublicationStatement);
                LOG.info("Publication created successfully.");
            } catch (Exception e) {
                LOG.info("Publication might already exist: {}", e.getMessage());
            }

            PGConnection pgConnection = conn.unwrap(PGConnection.class);

            // Get the replication connection
            PGReplicationConnection replicationConnection = pgConnection.getReplicationAPI();
            try {
                replicationConnection
                        .createReplicationSlot()
                        .logical()
                        .withSlotName("my_replication_slot")
                        .withOutputPlugin("pgoutput")
                        .make();
                LOG.info("Replication slot created successfully.");
            } catch (Exception e) {
                LOG.info("Replication slot might already exist: {}", e.getMessage());
            }

            // Create a replication stream
            PGReplicationStream stream = pgConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName("my_replication_slot")
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", "my_publication")
                    .start();

            if (eventProcessor != null) {
                while (true) {
                    // Read changes
                    ByteBuffer msg = stream.readPending();

                    if (msg == null) {
                        TimeUnit.MILLISECONDS.sleep(10L);
                        continue;
                    }

                    // decode and convert events to Data Prepper events
                    eventProcessor.process(msg);

                    // Acknowledge receiving the message
                    LogSequenceNumber lsn = stream.getLastReceiveLSN();
                    stream.setFlushedLSN(lsn);
                    stream.setAppliedLSN(lsn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void disconnect() {

    }


}
