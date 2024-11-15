/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import software.amazon.awssdk.services.rds.RdsClient;

public class BinlogClientFactory {

    private final RdsClient rdsClient;
    private final DbMetadata dbMetadata;
    private String username;
    private String password;
    private SSLMode sslMode = SSLMode.REQUIRED;

    public BinlogClientFactory(final RdsSourceConfig sourceConfig,
                               final RdsClient rdsClient,
                               final DbMetadata dbMetadata) {
        this.rdsClient = rdsClient;
        this.dbMetadata = dbMetadata;
        username = sourceConfig.getAuthenticationConfig().getUsername();
        password = sourceConfig.getAuthenticationConfig().getPassword();
    }

    public BinaryLogClient create() {
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

    public void setSSLMode(SSLMode sslMode) {
        this.sslMode = sslMode;
    }

    public void setCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }
}
