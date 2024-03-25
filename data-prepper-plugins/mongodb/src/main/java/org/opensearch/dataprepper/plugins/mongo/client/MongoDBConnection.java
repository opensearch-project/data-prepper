package org.opensearch.dataprepper.plugins.mongo.client;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.truststore.TrustStoreProvider;

import java.io.File;
import java.util.Objects;

public class MongoDBConnection {
    private static final String MONGO_CONNECTION_STRING_TEMPLATE = "mongodb://%s:%s@%s:%s/?replicaSet=rs0&readpreference=%s&ssl=%s&tlsAllowInvalidHostnames=%s&directConnection=%s";

    public static MongoClient getMongoClient(final MongoDBSourceConfig sourceConfig) {

        final String connectionString = getConnectionString(sourceConfig);

        final MongoClientSettings.Builder settingBuilder = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString));

        if (Objects.nonNull(sourceConfig.getTrustStoreFilePath())) {
            final File truststoreFilePath = new File(sourceConfig.getTrustStoreFilePath());
            settingBuilder.applyToSslSettings(builder -> {
                builder.enabled(sourceConfig.getInsecure());
                builder.invalidHostNameAllowed(sourceConfig.getSslInsecureDisableVerification());
                builder.context(TrustStoreProvider.createSSLContext(truststoreFilePath.toPath(),
                        sourceConfig.getTrustStorePassword()));
            });
        }

        return MongoClients.create(settingBuilder.build());
    }

    private static String getConnectionString(final MongoDBSourceConfig sourceConfig) {
        final String username = sourceConfig.getCredentialsConfig().getUsername();
        final String password = sourceConfig.getCredentialsConfig().getPassword();
        final String hostname = sourceConfig.getHostname();
        final int port = sourceConfig.getPort();
        final String ssl = sourceConfig.getInsecure().toString();
        final String invalidHostAllowed = sourceConfig.getSslInsecureDisableVerification().toString();
        final String readPreference = sourceConfig.getReadPreference();
        final String directionConnection = sourceConfig.getDirectConnection().toString();
        return String.format(MONGO_CONNECTION_STRING_TEMPLATE, username, password, hostname, port,
                readPreference, ssl, invalidHostAllowed, directionConnection);
    }
}
