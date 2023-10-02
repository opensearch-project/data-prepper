package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchClientFactory;

import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OpenSearchClientRefresher
        implements PluginComponentRefresher<OpenSearchClient, OpenSearchSourceConfiguration> {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final OpenSearchClientFactory openSearchClientFactory;
    private OpenSearchSourceConfiguration existingConfig;
    private OpenSearchClient currentClient;

    public OpenSearchClientRefresher(final OpenSearchClientFactory openSearchClientFactory,
                                     final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        this.openSearchClientFactory = openSearchClientFactory;
        existingConfig = openSearchSourceConfiguration;
        currentClient = openSearchClientFactory.provideOpenSearchClient(openSearchSourceConfiguration);
    }

    @Override
    public OpenSearchClient get() {
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
                currentClient = openSearchClientFactory.provideOpenSearchClient(openSearchSourceConfiguration);
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
