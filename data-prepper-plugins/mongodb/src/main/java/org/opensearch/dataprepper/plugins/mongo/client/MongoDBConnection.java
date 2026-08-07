package org.opensearch.dataprepper.plugins.mongo.client;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.truststore.TrustStoreProvider;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class MongoDBConnection {
    private static final String IAM_AUTH_SOURCE = "$external";
    private static final String IAM_AUTH_MECHANISM = "MONGODB-AWS";
    private static final String MONGO_PASSWORD_CONNECTION_STRING_TEMPLATE = "mongodb://%s:%s@%s:%s/?replicaSet=rs0&readpreference=%s&ssl=%s&tlsAllowInvalidHostnames=%s&directConnection=%s";
    private static final String MONGO_IAM_CONNECTION_STRING_TEMPLATE = "mongodb://%s:%s/?replicaSet=rs0&readpreference=%s&ssl=%s&tlsAllowInvalidHostnames=%s&directConnection=%s&authSource=%s&authMechanism=%s";

    public static MongoClient getMongoClient(final MongoDBSourceConfig sourceConfig) {

        final boolean usesIAMAuthentication = usesIAMAuthentication(sourceConfig);
        final String connectionString = getConnectionString(sourceConfig, usesIAMAuthentication);

        final MongoClientSettings.Builder settingBuilder = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString));
        if (usesIAMAuthentication) {
            // Create an empty credential. This triggers mongo to use the underlying IAM role.
            final MongoCredential credential = MongoCredential.createAwsCredential(null, null);
            settingBuilder.credential(credential);
        }

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

    private static String getConnectionString(final MongoDBSourceConfig sourceConfig, final boolean usesIamAuth) {
        // Support for only single host
        final String hostname = sourceConfig.getHost();
        final int port = sourceConfig.getPort();
        final String tls = sourceConfig.getTls().toString();
        final String invalidHostAllowed = sourceConfig.getSslInsecureDisableVerification().toString();
        final String readPreference = sourceConfig.getReadPreference();
        final String directionConnection = sourceConfig.getDirectConnection().toString();

        if (sourceConfig.getHost() == null || sourceConfig.getHost().isBlank()) {
            throw new RuntimeException("The host should not be null or empty.");
        }

        if (usesIamAuth) {
            return String.format(MONGO_IAM_CONNECTION_STRING_TEMPLATE, hostname, port, readPreference, tls, invalidHostAllowed, directionConnection, encodeString(IAM_AUTH_SOURCE), encodeString(IAM_AUTH_MECHANISM));
        }

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

        return String.format(MONGO_PASSWORD_CONNECTION_STRING_TEMPLATE, username, password, hostname, port, readPreference, tls, invalidHostAllowed, directionConnection);
    }

    private static boolean usesIAMAuthentication(final MongoDBSourceConfig sourceConfig) {
        final boolean hasUsernamePassword = Objects.nonNull(sourceConfig.getAuthenticationConfig()) &&
                (Objects.nonNull(sourceConfig.getAuthenticationConfig().getUsername()) ||
                 Objects.nonNull(sourceConfig.getAuthenticationConfig().getPassword()));
        
        if (hasUsernamePassword) {
            return false;
        }
        
        return Objects.nonNull(sourceConfig.getAwsConfig()) &&
               Objects.nonNull(sourceConfig.getAwsConfig().getAwsStsRoleArn());
    }
}
