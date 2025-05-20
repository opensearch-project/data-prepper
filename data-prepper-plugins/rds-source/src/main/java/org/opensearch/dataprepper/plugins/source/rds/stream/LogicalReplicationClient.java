/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.opensearch.dataprepper.plugins.source.rds.schema.ConnectionManager;
import org.opensearch.dataprepper.plugins.source.rds.utils.RdsSourceAggregateMetrics;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;

public class LogicalReplicationClient implements ReplicationLogClient {

    private static final Logger LOG = LoggerFactory.getLogger(LogicalReplicationClient.class);

    static final String PROTO_VERSION_KEY = "proto_version";
    static final String VERSION_ONE = "1";
    static final String PUBLICATION_NAMES_KEY = "publication_names";
    static final String AUTHENTICATION_FAILED = "authentication failed";
    static final String CONNECTION_REFUSED = "Connection refused";
    static final String REPLICATION_SLOT_DOES_NOT_EXIST = ".*replication slot .* does not exist.*";
    static final String PERMISSION_DENIED = "permission denied";

    private final ConnectionManager connectionManager;
    private final String publicationName;
    private final String replicationSlotName;
    private final RdsSourceAggregateMetrics rdsSourceAggregateMetrics;

    private LogSequenceNumber startLsn;
    private LogicalReplicationEventProcessor eventProcessor;

    private PGReplicationStream stream = null;
    private volatile boolean disconnectRequested = false;

    public LogicalReplicationClient(final ConnectionManager connectionManager,
                                    final String publicationName,
                                    final String replicationSlotName,
                                    final RdsSourceAggregateMetrics rdsSourceAggregateMetrics) {
        this.connectionManager = connectionManager;
        this.publicationName = publicationName;
        this.replicationSlotName = replicationSlotName;
        this.rdsSourceAggregateMetrics = rdsSourceAggregateMetrics;
    }

    @Override
    public void connect() {
        rdsSourceAggregateMetrics.getStreamApiInvocations().increment();
        LOG.debug("Start connecting logical replication stream. ");
        try (Connection conn = connectionManager.getConnection()) {
            PGConnection pgConnection = conn.unwrap(PGConnection.class);

            // Create a replication stream
            ChainedLogicalStreamBuilder logicalStreamBuilder = pgConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(replicationSlotName)
                    .withSlotOption(PROTO_VERSION_KEY, VERSION_ONE)
                    .withSlotOption(PUBLICATION_NAMES_KEY, publicationName);
            if (startLsn != null) {
                logicalStreamBuilder.withStartPosition(startLsn);
            }
            stream = logicalStreamBuilder.start();
            LOG.debug("Logical replication stream started. ");

            if (eventProcessor != null) {
                while (!disconnectRequested && !Thread.currentThread().isInterrupted()) {
                    try {
                        // Read changes
                        ByteBuffer msg = stream.readPending();

                        if (msg == null) {
                            Thread.sleep(10);
                            continue;
                        }

                        // decode and convert events to Data Prepper events
                        eventProcessor.process(msg);

                        // Acknowledge receiving the message
                        LogSequenceNumber lsn = stream.getLastReceiveLSN();
                        stream.setFlushedLSN(lsn);
                        stream.setAppliedLSN(lsn);
                    } catch (Exception e) {
                        LOG.error("Exception while processing Postgres replication stream. ");
                        closeStream();
                        throw e;
                    }
                }
            }

            closeStream();

            disconnectRequested = false;
        } catch (Exception e) {
            categorizeError(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void disconnect() {
        disconnectRequested = true;
        LOG.debug("Requested to disconnect logical replication stream.");

        closeStream();

        if (eventProcessor != null) {
            eventProcessor.stopCheckpointManager();
            LOG.debug("Stopped checkpoint manager.");
        }
    }

    public void setEventProcessor(LogicalReplicationEventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    public void setStartLsn(LogSequenceNumber startLsn) {
        this.startLsn = startLsn;
    }

    private void closeStream() {
        if (stream != null && !stream.isClosed()) {
            try {
                stream.close();
                LOG.debug("Replication stream closed.");
            } catch (Exception e) {
                LOG.error("Exception while closing replication stream. ", e);
            }
        }
    }

    private void categorizeError(Exception e) {
        if (e.getMessage() != null && e.getMessage().contains(AUTHENTICATION_FAILED)) {
            rdsSourceAggregateMetrics.getStream4xxErrors().increment();
            rdsSourceAggregateMetrics.getStreamAuthErrors().increment();
            LOG.error("Failed to create or process PostgreSQL replication stream: Authentication failed. [{}]", e.getMessage());
        } else if (e.getCause() != null && e.getCause().getMessage() != null && e.getCause().getMessage().contains(CONNECTION_REFUSED)) {
            rdsSourceAggregateMetrics.getStream4xxErrors().increment();
            rdsSourceAggregateMetrics.getStreamServerNotFoundErrors().increment();
            LOG.error("Failed to create or process PostgreSQL replication stream: Cannot connect to PostgreSQL server. [{}]", e.getMessage());
        } else if (e.getMessage() != null && e.getMessage().matches(REPLICATION_SLOT_DOES_NOT_EXIST)) {
            rdsSourceAggregateMetrics.getStream4xxErrors().increment();
            rdsSourceAggregateMetrics.getStreamReplicationNotEnabledErrors().increment();
            LOG.error("Failed to create or process PostgreSQL replication stream: Replication slot does not exist. [{}]", e.getMessage());
        } else if (e.getMessage() != null && e.getMessage().contains(PERMISSION_DENIED)) {
            rdsSourceAggregateMetrics.getStream4xxErrors().increment();
            rdsSourceAggregateMetrics.getStreamAccessDeniedErrors().increment();
            LOG.error("Failed to create or process PostgreSQL replication stream: Insufficient privileges. [{}]", e.getMessage());
        } else {
            rdsSourceAggregateMetrics.getStream5xxErrors().increment();
            LOG.error("Failed to create or process PostgreSQL replication stream. ", e);
        }
    }
}
