/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import software.amazon.awssdk.services.rds.RdsClient;

import java.util.List;
import java.util.NoSuchElementException;

public class ReplicationLogClientFactory {

    private final RdsClient rdsClient;
    private final DbMetadata dbMetadata;
    private final EngineType engineType;
    private String username;
    private String password;
    private String database;
    private SSLMode sslMode = SSLMode.REQUIRED;

    public ReplicationLogClientFactory(final RdsSourceConfig sourceConfig,
                                       final RdsClient rdsClient,
                                       final DbMetadata dbMetadata) {
        this.rdsClient = rdsClient;
        this.dbMetadata = dbMetadata;
        engineType = sourceConfig.getEngine();
        username = sourceConfig.getAuthenticationConfig().getUsername();
        password = sourceConfig.getAuthenticationConfig().getPassword();
        if (sourceConfig.getEngine() == EngineType.POSTGRES) {
            database = getDatabaseName(sourceConfig.getTableNames());
        }
    }

    public ReplicationLogClient create(StreamPartition streamPartition) {
        if (engineType == EngineType.MYSQL) {
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
        final String replicationSlotName = streamPartition.getProgressState().get().getReplicationSlotName();
        if (replicationSlotName == null) {
            throw new NoSuchElementException("Replication slot name is not found in progress state.");
        }
        return new LogicalReplicationClient(
                dbMetadata.getEndpoint(),
                dbMetadata.getPort(),
                username,
                password,
                database,
                replicationSlotName);
    }

    public void setSSLMode(SSLMode sslMode) {
        this.sslMode = sslMode;
    }

    public void setCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

    private String getDatabaseName(List<String> tableNames) {
        return tableNames.get(0).split("\\.")[0];
    }
}
