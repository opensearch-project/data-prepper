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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.schema.PostgresConnectionManager;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LogicalReplicationClientTest {

    @Mock
    private PostgresConnectionManager connectionManager;

    @Mock
    private LogicalReplicationEventProcessor eventProcessor;

    private String replicationSlotName;
    private LogicalReplicationClient logicalReplicationClient;

    @BeforeEach
    void setUp() {
        replicationSlotName = UUID.randomUUID().toString();
        logicalReplicationClient = createObjectUnderTest();
        logicalReplicationClient.setEventProcessor(eventProcessor);
    }

    @Test
    void test_connect() throws SQLException, InterruptedException {
        final Connection connection = mock(Connection.class);
        final PGConnection pgConnection = mock(PGConnection.class, RETURNS_DEEP_STUBS);
        final ChainedLogicalStreamBuilder logicalStreamBuilder = mock(ChainedLogicalStreamBuilder.class);
        final PGReplicationStream stream = mock(PGReplicationStream.class);
        final ByteBuffer message = mock(ByteBuffer.class);
        final LogSequenceNumber lsn = mock(LogSequenceNumber.class);

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.unwrap(PGConnection.class)).thenReturn(pgConnection);
        when(pgConnection.getReplicationAPI().replicationStream().logical()).thenReturn(logicalStreamBuilder);
        when(logicalStreamBuilder.withSlotName(anyString())).thenReturn(logicalStreamBuilder);
        when(logicalStreamBuilder.withSlotOption(anyString(), anyString())).thenReturn(logicalStreamBuilder);
        when(logicalStreamBuilder.start()).thenReturn(stream);
        when(stream.readPending()).thenReturn(message).thenReturn(null);
        when(stream.getLastReceiveLSN()).thenReturn(lsn);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> logicalReplicationClient.connect());

        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(eventProcessor).process(message));
        Thread.sleep(20);
        executorService.shutdownNow();

        verify(stream).setAppliedLSN(lsn);
        verify(stream).setFlushedLSN(lsn);
    }

    private LogicalReplicationClient createObjectUnderTest() {
         return new LogicalReplicationClient(connectionManager, replicationSlotName);
    }
}
