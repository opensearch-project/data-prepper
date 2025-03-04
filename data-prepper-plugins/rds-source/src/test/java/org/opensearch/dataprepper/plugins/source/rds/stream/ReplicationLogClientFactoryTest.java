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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.PostgresStreamState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import software.amazon.awssdk.services.rds.RdsClient;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReplicationLogClientFactoryTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RdsSourceConfig sourceConfig;

    @Mock
    private RdsClient rdsClient;

    @Mock
    private DbMetadata dbMetadata;

    @Mock
    private StreamPartition streamPartition;

    private ReplicationLogClientFactory replicationLogClientFactory;

    @Test
    void test_create_binlog_client() {
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();

        when(sourceConfig.getEngine()).thenReturn(EngineType.MYSQL);
        when(sourceConfig.getAuthenticationConfig().getUsername()).thenReturn(username);
        when(sourceConfig.getAuthenticationConfig().getPassword()).thenReturn(password);

        replicationLogClientFactory = createObjectUnderTest();
        ReplicationLogClient replicationLogClient = replicationLogClientFactory.create(streamPartition);

        verify(dbMetadata).getEndpoint();
        verify(dbMetadata).getPort();
        assertThat(replicationLogClient, instanceOf(BinlogClientWrapper.class));
    }

    @Test
    void test_create_logical_replication_client() {
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        final StreamProgressState streamProgressState = mock(StreamProgressState.class);
        final PostgresStreamState postgresStreamState = mock(PostgresStreamState.class);
        final String slotName = UUID.randomUUID().toString();
        final List<String> tableNames = List.of("table1", "table2");

        when(sourceConfig.getEngine()).thenReturn(EngineType.POSTGRES);
        when(sourceConfig.isTlsEnabled()).thenReturn(true);
        when(sourceConfig.getAuthenticationConfig().getUsername()).thenReturn(username);
        when(sourceConfig.getAuthenticationConfig().getPassword()).thenReturn(password);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        when(streamProgressState.getPostgresStreamState()).thenReturn(postgresStreamState);
        when(postgresStreamState.getReplicationSlotName()).thenReturn(slotName);

        replicationLogClientFactory = createObjectUnderTest();
        ReplicationLogClient replicationLogClient = replicationLogClientFactory.create(streamPartition);

        verify(dbMetadata).getEndpoint();
        verify(dbMetadata).getPort();
        assertThat(replicationLogClient, instanceOf(LogicalReplicationClient.class));
    }

    private ReplicationLogClientFactory createObjectUnderTest() {
        return new ReplicationLogClientFactory(sourceConfig, rdsClient, dbMetadata);
    }
}
