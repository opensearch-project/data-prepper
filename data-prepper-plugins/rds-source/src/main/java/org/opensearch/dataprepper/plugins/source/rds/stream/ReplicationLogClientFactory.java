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
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import software.amazon.awssdk.services.rds.RdsClient;

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
        database = "my_db";  // TODO: extact database name from source config
    }

    public ReplicationLogClient create() {
        if (engineType == EngineType.MYSQL) {
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
            return new BinlogClientWrapper(binaryLogClient);
        } else if (engineType == EngineType.POSTGRES) {
            return new LogicalReplicationClient(
                    dbMetadata.getEndpoint(),
                    dbMetadata.getPort(),
                    username,
                    password,
                    database);
        } else {
            throw new UnsupportedOperationException("Unsupported engine type: " + engineType);
        }
    }

    public void setSSLMode(SSLMode sslMode) {
        this.sslMode = sslMode;
    }

    public void setCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }
}
