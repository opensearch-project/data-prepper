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

    private final ConnectionManager connectionManager;
    private final String publicationName;
    private final String replicationSlotName;
    private LogSequenceNumber startLsn;
    private LogicalReplicationEventProcessor eventProcessor;

    private PGReplicationStream stream = null;
    private volatile boolean disconnectRequested = false;

    public LogicalReplicationClient(final ConnectionManager connectionManager,
                                    final String publicationName,
                                    final String replicationSlotName) {
        this.connectionManager = connectionManager;
        this.publicationName = publicationName;
        this.replicationSlotName = replicationSlotName;
    }

    @Override
    public void connect() {
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
                        LOG.error("Exception while processing Postgres replication stream. ", e);
                        closeStream();
                        throw e;
                    }
                }
            }

            closeStream();

            disconnectRequested = false;
        } catch (Exception e) {
            LOG.error("Exception while creating or processing Postgres replication stream. ", e);
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
}
