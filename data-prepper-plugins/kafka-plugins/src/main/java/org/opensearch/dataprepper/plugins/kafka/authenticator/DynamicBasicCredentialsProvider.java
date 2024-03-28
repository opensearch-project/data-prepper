package org.opensearch.dataprepper.plugins.kafka.authenticator;

import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;

import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DynamicBasicCredentialsProvider {
    private static final DynamicBasicCredentialsProvider singleton = new DynamicBasicCredentialsProvider();

    public static DynamicBasicCredentialsProvider getInstance() {
        return singleton;
    }

    private BasicCredentials basicCredentials;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    // Used for testing only
    protected DynamicBasicCredentialsProvider() {}

    public BasicCredentials getBasicCredentials() {
        readWriteLock.readLock().lock();
        try {
            return basicCredentials;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public void refresh(final KafkaConnectionConfig newConfig) {
        final AuthConfig authConfig = newConfig.getAuthConfig();
        if (Objects.nonNull(authConfig)) {
            AuthConfig.SaslAuthConfig saslAuthConfig = authConfig.getSaslAuthConfig();
            if (Objects.nonNull(saslAuthConfig) && Objects.nonNull(saslAuthConfig.getPlainTextAuthConfig())) {
                final PlainTextAuthConfig plainTextAuthConfig = newConfig.getAuthConfig().getSaslAuthConfig()
                        .getPlainTextAuthConfig();
                final String newUsername = plainTextAuthConfig.getUsername();
                final String newPassword = plainTextAuthConfig.getPassword();
                readWriteLock.writeLock().lock();
                try {
                    basicCredentials = new BasicCredentials(newUsername, newPassword);
                } finally {
                    readWriteLock.writeLock().unlock();
                }
            }
        }
    }
}
