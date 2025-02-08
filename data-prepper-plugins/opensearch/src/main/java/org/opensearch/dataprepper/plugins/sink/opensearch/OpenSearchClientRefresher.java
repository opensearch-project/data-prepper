package org.opensearch.dataprepper.plugins.sink.opensearch;

import io.micrometer.core.instrument.Counter;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class OpenSearchClientRefresher implements PluginComponentRefresher<OpenSearchClient, OpenSearchSinkConfig> {
    static final String CREDENTIALS_CHANGED = "credentialsChanged";
    static final String CLIENT_REFRESH_ERRORS = "clientRefreshErrors";
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchClientRefresher.class);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Function<ConnectionConfiguration, OpenSearchClient> clientFunction;
    private OpenSearchClient currentClient;
    private ConnectionConfiguration currentConfig;

    private final Counter credentialsChangeCounter;
    private final Counter clientRefreshErrorsCounter;

    public OpenSearchClientRefresher(final PluginMetrics pluginMetrics,
                                     final ConnectionConfiguration connectionConfiguration,
                                     final Function<ConnectionConfiguration, OpenSearchClient> clientFunction) {
        this.clientFunction = clientFunction;
        this.currentConfig = connectionConfiguration;
        this.currentClient = null;
        credentialsChangeCounter = pluginMetrics.counter(CREDENTIALS_CHANGED);
        clientRefreshErrorsCounter = pluginMetrics.counter(CLIENT_REFRESH_ERRORS);
    }

    @Override
    public Class<OpenSearchClient> getComponentClass() {
        return OpenSearchClient.class;
    }

    @Override
    public OpenSearchClient get() {
        readWriteLock.readLock().lock();
        try {
            if (currentClient == null) {
                currentClient = clientFunction.apply(currentConfig);
            }
            return currentClient;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void update(OpenSearchSinkConfig openSearchSinkConfig) {
        final ConnectionConfiguration newConfig = ConnectionConfiguration.readConnectionConfiguration(openSearchSinkConfig);
        if (basicAuthChanged(newConfig)) {
            credentialsChangeCounter.increment();
            readWriteLock.writeLock().lock();
            try {
                currentClient = clientFunction.apply(newConfig);
                currentConfig = newConfig;
            } catch (Exception e) {
                clientRefreshErrorsCounter.increment();
                LOG.error("Refreshing {} failed.", getComponentClass(), e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    private boolean basicAuthChanged(final ConnectionConfiguration newConfig) {
        final String existingUsername;
        final String existingPassword;
        if (currentConfig.getAuthConfig() != null) {
            existingUsername = currentConfig.getAuthConfig().getUsername();
            existingPassword = currentConfig.getAuthConfig().getPassword();
        } else {
            existingUsername = currentConfig.getUsername();
            existingPassword = currentConfig.getPassword();
        }

        final String newUsername;
        final String newPassword;
        if (newConfig.getAuthConfig() != null) {
            newUsername = newConfig.getAuthConfig().getUsername();
            newPassword = newConfig.getAuthConfig().getPassword();
        } else {
            newUsername = newConfig.getUsername();
            newPassword = newConfig.getPassword();
        }

        return !Objects.equals(existingUsername, newUsername) ||
                !Objects.equals(existingPassword, newPassword);
    }
}
