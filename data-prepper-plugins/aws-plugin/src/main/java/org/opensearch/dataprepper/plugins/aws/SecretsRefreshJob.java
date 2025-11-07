/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.LimitExceededException;

public class SecretsRefreshJob implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SecretsRefreshJob.class);
    static final String SECRETS_REFRESH_SUCCESS = "secretsRefreshSuccess";
    static final String SECRETS_REFRESH_FAILURE = "secretsRefreshFailure";
    static final String SECRETS_REFRESH_DURATION = "secretsRefreshDuration";
    static final String SECRETS_MANAGER_RESOURCE_NOT_FOUND = "secretsManagerResourceNotFound";
    static final String SECRETS_MANAGER_LIMIT_EXCEEDED = "secretsManagerLimitExceeded";
    static final String SECRET_CONFIG_ID_TAG = "secretConfigId";
    private final String secretConfigId;
    private final SecretsSupplier secretsSupplier;
    private final PluginConfigPublisher pluginConfigPublisher;
    private final PluginMetrics pluginMetrics;
    private final Counter secretsRefreshSuccessCounter;
    private final Counter secretsRefreshFailureCounter;
    private final Timer secretsRefreshTimer;
    private final Counter secretsManagerResourceNotFoundCounter;
    private final Counter secretsManagerLimitExceededCounter;

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
        this.secretsManagerResourceNotFoundCounter = pluginMetrics.counter(
                SECRETS_MANAGER_RESOURCE_NOT_FOUND);
        this.secretsManagerLimitExceededCounter = pluginMetrics.counter(
                SECRETS_MANAGER_LIMIT_EXCEEDED);
    }

    @Override
    public void run() {
        secretsRefreshTimer.record(() -> {
            try {
                secretsSupplier.refresh(secretConfigId);
                secretsRefreshSuccessCounter.increment();
                pluginConfigPublisher.notifyAllPluginConfigObservable();
            } catch(Exception e) {
                if (e instanceof SecretsManagerException) {
                    recordSecretsManagerException((SecretsManagerException) e);
                }
                LOG.error("Failed to refresh secrets in aws:secrets:{}.", secretConfigId, e);
                secretsRefreshFailureCounter.increment();
            }
        });
    }

    /**
     * Records metrics for Secrets Manager exceptions based on the exception type.
     * 
     * @param e the Secrets Manager exception to record metrics for
     */
    private void recordSecretsManagerException(final SecretsManagerException e) {
        if (e instanceof ResourceNotFoundException) {
            secretsManagerResourceNotFoundCounter.increment();
        } else if (e instanceof LimitExceededException) {
            secretsManagerLimitExceededCounter.increment();
        }
    }
}
