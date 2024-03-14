package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

public class OpenSearchClientRefresher implements PluginComponentRefresher<OpenSearchClient, PluginSetting> {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchClientRefresher.class);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final BiFunction<AwsCredentialsSupplier, ConnectionConfiguration, OpenSearchClient> clientBiFunction;
    private OpenSearchClient currentClient;
    private ConnectionConfiguration currentConfig;

    public OpenSearchClientRefresher(final AwsCredentialsSupplier awsCredentialsSupplier,
                                     final OpenSearchClient openSearchClient,
                                     final ConnectionConfiguration connectionConfiguration,
                                     final BiFunction<AwsCredentialsSupplier, ConnectionConfiguration, OpenSearchClient>
                                             clientBiFunction) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.clientBiFunction = clientBiFunction;
        this.currentConfig = connectionConfiguration;
        this.currentClient = openSearchClient;
    }

    @Override
    public Class<OpenSearchClient> getComponentClass() {
        return OpenSearchClient.class;
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
    public void update(PluginSetting pluginSetting) {
        final ConnectionConfiguration newConfig = ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        if (basicAuthChanged(newConfig)) {
            readWriteLock.writeLock().lock();
            try {
                currentClient = clientBiFunction.apply(awsCredentialsSupplier, newConfig);
                currentConfig = newConfig;
            } catch (Exception e) {
                LOG.error("Refreshing {} failed.", getComponentClass(), e);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    private boolean basicAuthChanged(final ConnectionConfiguration newConfig) {
        return !Objects.equals(currentConfig.getUsername(), newConfig.getUsername()) ||
                !Objects.equals(currentConfig.getPassword(), newConfig.getPassword());
    }
}
