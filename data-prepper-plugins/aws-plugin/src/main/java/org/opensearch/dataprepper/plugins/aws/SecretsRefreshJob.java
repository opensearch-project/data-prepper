package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;

public class SecretsRefreshJob implements Runnable {
    private final String secretConfigId;
    private final SecretsSupplier secretsSupplier;
    private final PluginConfigPublisher pluginConfigPublisher;

    public SecretsRefreshJob(final String secretConfigId,
                             final SecretsSupplier secretsSupplier,
                             final PluginConfigPublisher pluginConfigPublisher) {
        this.secretConfigId = secretConfigId;
        this.secretsSupplier = secretsSupplier;
        this.pluginConfigPublisher = pluginConfigPublisher;
    }

    @Override
    public void run() {
        secretsSupplier.refresh(secretConfigId);
        pluginConfigPublisher.notifyAllPluginConfigurationObservable();
    }
}
