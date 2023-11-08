/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;

public class MongoDBHelper {

    public static MongoClient getMongoClient(final MongoDBConfig mongoDBConfig) {
        String template = "mongodb://%s:%s@%s:%s/?replicaSet=rs0&directConnection=true&readpreference=%s&ssl=%s&tlsAllowInvalidHostnames=%s";
        String username = mongoDBConfig.getCredentialsConfig().getUsername();
        String password = mongoDBConfig.getCredentialsConfig().getPassword();
        String hostname = mongoDBConfig.getHostname();
        String port = mongoDBConfig.getPort();
        String ssl = mongoDBConfig.getSSLEnabled().toString();
        String invalidHostAllowed = mongoDBConfig.getSSLInvalidHostAllowed().toString();
        String readPreference = mongoDBConfig.getExportConfig().getReadPreference();
        String connectionString = String.format(template, username, password, hostname, port, readPreference, ssl, invalidHostAllowed);

        return MongoClients.create(connectionString);
    }

    public static Bson buildQuery(String gte, String lte, String className) {
        switch (className) {
            case "java.lang.Integer":
                return and(
                        gte("_id", Integer.parseInt(gte)),
                        lte("_id", Integer.parseInt(lte))
                );
            case "java.lang.Double":
                return and(
                        gte("_id", Double.parseDouble(gte)),
                        lte("_id", Double.parseDouble(lte))
                );
            case "java.lang.String":
                return and(
                        gte("_id", gte),
                        lte("_id", lte)
                );
            case "java.lang.Long":
                return and(
                        gte("_id", Long.parseLong(gte)),
                        lte("_id", Long.parseLong(lte))
                );
            case "org.bson.types.ObjectId":
                return and(
                        gte("_id", new ObjectId(gte)),
                        lte("_id", new ObjectId(lte))
                );
            default:
                throw new RuntimeException("Unexpected _id class supported: " + className);
        }
    }
}
