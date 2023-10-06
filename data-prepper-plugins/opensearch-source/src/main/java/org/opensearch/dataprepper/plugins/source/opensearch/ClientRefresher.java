package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;

import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class ClientRefresher<Client>
        implements PluginComponentRefresher<Client, OpenSearchSourceConfiguration> {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Function<OpenSearchSourceConfiguration, Client> clientFunction;
    private OpenSearchSourceConfiguration existingConfig;
    private final Class<Client> clientClass;
    private Client currentClient;

    public ClientRefresher(final Class<Client> clientClass,
                           final Function<OpenSearchSourceConfiguration, Client> clientFunction,
                           final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
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
            readWriteLock.writeLock().lock();
            try {
                currentClient = clientFunction.apply(openSearchSourceConfiguration);
                existingConfig = openSearchSourceConfiguration;
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    private boolean basicAuthChanged(final OpenSearchSourceConfiguration newConfig) {
        return !Objects.equals(existingConfig.getUsername(), newConfig.getUsername()) ||
                !Objects.equals(existingConfig.getPassword(), newConfig.getPassword());
    }
}
