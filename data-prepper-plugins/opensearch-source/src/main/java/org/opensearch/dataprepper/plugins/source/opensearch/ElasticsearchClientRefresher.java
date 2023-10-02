package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchClientFactory;

import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ElasticsearchClientRefresher
        implements PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final OpenSearchClientFactory openSearchClientFactory;
    private OpenSearchSourceConfiguration existingConfig;
    private ElasticsearchClient currentClient;

    public ElasticsearchClientRefresher(final OpenSearchClientFactory openSearchClientFactory,
                                        final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        this.openSearchClientFactory = openSearchClientFactory;
        existingConfig = openSearchSourceConfiguration;
        currentClient = openSearchClientFactory.provideElasticSearchClient(openSearchSourceConfiguration);
    }

    @Override
    public ElasticsearchClient get() {
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
                currentClient = openSearchClientFactory.provideElasticSearchClient(openSearchSourceConfiguration);
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
