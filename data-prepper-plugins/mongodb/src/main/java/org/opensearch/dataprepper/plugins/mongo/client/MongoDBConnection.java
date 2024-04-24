package org.opensearch.dataprepper.plugins.mongo.client;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.truststore.TrustStoreProvider;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
                builder.enabled(sourceConfig.getTls());
                builder.invalidHostNameAllowed(sourceConfig.getSslInsecureDisableVerification());
                builder.context(TrustStoreProvider.createSSLContext(truststoreFilePath.toPath(),
                        sourceConfig.getTrustStorePassword()));
            });
        }

        return MongoClients.create(settingBuilder.build());
    }

    private static String encodeString(final String input) {
        return URLEncoder.encode(input, StandardCharsets.UTF_8);
    }

    private static String getConnectionString(final MongoDBSourceConfig sourceConfig) {
        final String username;
        try {
            username = encodeString(sourceConfig.getAuthenticationConfig().getUsername());
        } catch (final Exception e) {
            throw new RuntimeException("Unsupported characters in username.");
        }

        final String password;
        try {
            password = encodeString(sourceConfig.getAuthenticationConfig().getPassword());
        } catch (final Exception e) {
            throw new RuntimeException("Unsupported characters in password.");
        }

        if (sourceConfig.getHost() == null || sourceConfig.getHost().isBlank()) {
            throw new RuntimeException("The host should not be null or empty.");
        }

        // Support for only single host
        final String hostname = sourceConfig.getHost();
        final int port = sourceConfig.getPort();
        final String tls = sourceConfig.getTls().toString();
        final String invalidHostAllowed = sourceConfig.getSslInsecureDisableVerification().toString();
        final String readPreference = sourceConfig.getReadPreference();
        final String directionConnection = sourceConfig.getDirectConnection().toString();
        return String.format(MONGO_CONNECTION_STRING_TEMPLATE, username, password, hostname, port,
                readPreference, tls, invalidHostAllowed, directionConnection);
    }
}
