package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
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
    private final String replicationSlotName;
    private Properties props;
    private LogSequenceNumber startLsn;
    private PostgresReplicationEventProcessor eventProcessor;

    private volatile boolean disconnectRequested = false;

    public LogicalReplicationClient(final String endpoint,
                                    final int port,
                                    final String username,
                                    final String password,
                                    final String database,
                                    final String replicationSlotName) {
//        this.endpoint = endpoint;
//        this.port = port;
        this.endpoint = "127.0.0.1";
        this.port = 5432;
        this.username = username;
        this.password = password;
        this.database = database;
        this.replicationSlotName = replicationSlotName;
        jdbcUrl = String.format(URL_FORMAT, this.endpoint, this.port, this.database);
        props = new Properties();
    }

    public void setEventProcessor(PostgresReplicationEventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    public void setStartLsn(LogSequenceNumber startLsn) {
        this.startLsn = startLsn;
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
            PGConnection pgConnection = conn.unwrap(PGConnection.class);

            // Create a replication stream
            ChainedLogicalStreamBuilder logicalStreamBuilder = pgConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(replicationSlotName)
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", "my_publication");
            if (startLsn != null) {
                logicalStreamBuilder.withStartPosition(startLsn);
            }
            PGReplicationStream stream = logicalStreamBuilder.start();

            if (eventProcessor != null) {
                while (!disconnectRequested) {
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

            stream.close();
            LOG.info("Replication stream closed successfully.");
        } catch (Exception e) {
            LOG.error("Exception while reading Postgres replication stream. ", e);
        }
    }

    public void disconnect() {
        disconnectRequested = true;
    }
}
