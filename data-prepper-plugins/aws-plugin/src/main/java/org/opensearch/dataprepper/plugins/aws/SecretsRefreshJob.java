package org.opensearch.dataprepper.plugins.aws;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecretsRefreshJob implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SecretsRefreshJob.class);
    static final String SECRETS_REFRESH_SUCCESS = "secretsRefreshSuccess";
    static final String SECRETS_REFRESH_FAILURE = "secretsRefreshFailure";
    static final String SECRETS_REFRESH_DURATION = "secretsRefreshDuration";
    static final String SECRET_CONFIG_ID_TAG = "secretConfigId";
    private final String secretConfigId;
    private final SecretsSupplier secretsSupplier;
    private final PluginConfigPublisher pluginConfigPublisher;
    private final PluginMetrics pluginMetrics;
    private final Counter secretsRefreshSuccessCounter;
    private final Counter secretsRefreshFailureCounter;
    private final Timer secretsRefreshTimer;

    public SecretsRefreshJob(final String secretConfigId,
                             final SecretsSupplier secretsSupplier,
                             final PluginConfigPublisher pluginConfigPublisher,
                             final PluginMetrics pluginMetrics) {
        this.secretConfigId = secretConfigId;
        this.secretsSupplier = secretsSupplier;
        this.pluginConfigPublisher = pluginConfigPublisher;
        this.pluginMetrics = pluginMetrics;
        this.secretsRefreshSuccessCounter = pluginMetrics.counterWithTags(
                SECRETS_REFRESH_SUCCESS, SECRET_CONFIG_ID_TAG, secretConfigId);
        this.secretsRefreshFailureCounter = pluginMetrics.counterWithTags(
                SECRETS_REFRESH_FAILURE, SECRET_CONFIG_ID_TAG, secretConfigId);
        this.secretsRefreshTimer = pluginMetrics.timerWithTags(
                SECRETS_REFRESH_DURATION, SECRET_CONFIG_ID_TAG, secretConfigId);
    }

    @Override
    public void run() {
        secretsRefreshTimer.record(() -> {
            try {
                secretsSupplier.refresh(secretConfigId);
                secretsRefreshSuccessCounter.increment();
                pluginConfigPublisher.notifyAllPluginConfigObservable();
            } catch(Exception e) {
                LOG.error("Failed to refresh secrets in aws:secrets:{}.", secretConfigId, e);
                secretsRefreshFailureCounter.increment();
            }
        });
    }
}
