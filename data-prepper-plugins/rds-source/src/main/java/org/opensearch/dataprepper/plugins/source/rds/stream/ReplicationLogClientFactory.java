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

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.PostgresStreamState;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import org.opensearch.dataprepper.plugins.source.rds.schema.ConnectionManager;
import org.opensearch.dataprepper.plugins.source.rds.schema.ConnectionManagerFactory;
import software.amazon.awssdk.services.rds.RdsClient;

import java.util.List;
import java.util.NoSuchElementException;

public class ReplicationLogClientFactory {

    private final RdsClient rdsClient;
    private final DbMetadata dbMetadata;
    private RdsSourceConfig sourceConfig;
    private String username;
    private String password;
    private SSLMode sslMode = SSLMode.REQUIRED;

    public ReplicationLogClientFactory(final RdsSourceConfig sourceConfig,
                                       final RdsClient rdsClient,
                                       final DbMetadata dbMetadata) {
        this.sourceConfig = sourceConfig;
        this.rdsClient = rdsClient;
        this.dbMetadata = dbMetadata;
        username = sourceConfig.getAuthenticationConfig().getUsername();
        password = sourceConfig.getAuthenticationConfig().getPassword();
    }

    public ReplicationLogClient create(StreamPartition streamPartition) {
        if (sourceConfig.getEngine().isMySql()) {
            return new BinlogClientWrapper(createBinaryLogClient());
        } else { // Postgres
            return createLogicalReplicationClient(streamPartition);
        }
    }

    private BinaryLogClient createBinaryLogClient() {
        BinaryLogClient binaryLogClient = new BinaryLogClient(
                dbMetadata.getEndpoint(),
                dbMetadata.getPort(),
                username,
                password);
        binaryLogClient.setSSLMode(sslMode);
        final EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );
        binaryLogClient.setEventDeserializer(eventDeserializer);
        return binaryLogClient;
    }

    private LogicalReplicationClient createLogicalReplicationClient(StreamPartition streamPartition) {
        final PostgresStreamState postgresStreamState = streamPartition.getProgressState().get().getPostgresStreamState();
        final String publicationName = postgresStreamState.getPublicationName();
        final String replicationSlotName = postgresStreamState.getReplicationSlotName();
        if (replicationSlotName == null) {
            throw new NoSuchElementException("Replication slot name is not found in progress state.");
        }
        final ConnectionManagerFactory connectionManagerFactory = new ConnectionManagerFactory(sourceConfig, dbMetadata);
        final ConnectionManager connectionManager = connectionManagerFactory.getConnectionManager();
        return new LogicalReplicationClient(connectionManager, publicationName, replicationSlotName);
    }

    public void setSSLMode(SSLMode sslMode) {
        this.sslMode = sslMode;
    }

    public void updateCredentials(RdsSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        this.username = sourceConfig.getAuthenticationConfig().getUsername();
        this.password = sourceConfig.getAuthenticationConfig().getPassword();
    }

    private String getDatabaseName(List<String> tableNames) {
        return tableNames.get(0).split("\\.")[0];
    }
}
