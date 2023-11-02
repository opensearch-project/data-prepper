/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;

public class MongoDBHelper {

    public static MongoClient getMongoClient(final MongoDBConfig mongoDBConfig) {
        String template = "mongodb://%s:%s@%s:%s/?replicaSet=rs0&directConnection=true&readpreference=%s&ssl=%s&tlsAllowInvalidHostnames=%s";
        String username = mongoDBConfig.getCredentialsConfig().getUsername();
        String password = mongoDBConfig.getCredentialsConfig().getPassword();
        String hostname = mongoDBConfig.getHostname();
        String port = mongoDBConfig.getPort();
        String ssl = mongoDBConfig.getSSLEnabled().toString();
        String invalidHostAllowed = mongoDBConfig.getSSLInvalidHostAllowed().toString();
        String readPreference = "secondaryPreferred";
        String connectionString = String.format(template, username, password, hostname, port, readPreference, ssl, invalidHostAllowed);

        return MongoClients.create(connectionString);
    }
}
