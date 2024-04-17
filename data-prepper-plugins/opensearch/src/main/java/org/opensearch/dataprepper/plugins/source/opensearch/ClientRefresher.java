package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.opensearch.dataprepper.plugins.source.opensearch.metrics.OpenSearchSourcePluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class ClientRefresher<Client>
        implements PluginComponentRefresher<Client, OpenSearchSourceConfiguration> {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRefresher.class);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics;
    private final Function<OpenSearchSourceConfiguration, Client> clientFunction;
    private OpenSearchSourceConfiguration existingConfig;
    private final Class<Client> clientClass;
    private Client currentClient;

    public ClientRefresher(final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics,
                           final Class<Client> clientClass,
                           final Function<OpenSearchSourceConfiguration, Client> clientFunction,
                           final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        this.openSearchSourcePluginMetrics = openSearchSourcePluginMetrics;
        this.clientClass = clientClass;
        this.clientFunction = clientFunction;
        existingConfig = openSearchSourceConfiguration;
        currentClient = clientFunction.apply(openSearchSourceConfiguration);
    }

    @Override
    public Class<Client> getComponentClass() {
        return clientClass;
    }

    @Override
    public Client get() {
        readWriteLock.readLock().lock();
        try {
            return currentClient;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void update(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        if (basicAuthChanged(openSearchSourceConfiguration)) {
            openSearchSourcePluginMetrics.getCredentialsChangeCounter().increment();
            readWriteLock.writeLock().lock();
            try {
                currentClient = clientFunction.apply(openSearchSourceConfiguration);
                existingConfig = openSearchSourceConfiguration;
            } catch (Exception e) {
                openSearchSourcePluginMetrics.getClientRefreshErrorsCounter().increment();
                LOG.error("Refreshing {} failed.", getComponentClass(), e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    private boolean basicAuthChanged(final OpenSearchSourceConfiguration newConfig) {
        final String existingUsername;
        final String existingPassword;
        if (existingConfig.getAuthConfig() != null) {
            existingUsername = existingConfig.getAuthConfig().getUsername();
            existingPassword = existingConfig.getAuthConfig().getPassword();
        } else {
            existingUsername = existingConfig.getUsername();
            existingPassword = existingConfig.getPassword();
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
