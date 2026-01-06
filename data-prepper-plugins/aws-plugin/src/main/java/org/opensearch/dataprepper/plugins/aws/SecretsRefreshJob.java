/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
    static final String SECRETS_SECRET_NOT_FOUND = "secretsSecretNotFound";
    static final String SECRETS_LIMIT_EXCEEDED = "secretsLimitExceeded";
    static final String SECRETS_ACCESS_DENIED = "secretsAccessDenied";
    static final String SECRET_CONFIG_ID_TAG = "secretConfigId";
    private final String secretConfigId;
    private final SecretsSupplier secretsSupplier;
    private final PluginConfigPublisher pluginConfigPublisher;
    private final PluginMetrics pluginMetrics;
    private final Counter secretsRefreshSuccessCounter;
    private final Counter secretsRefreshFailureCounter;
    private final Timer secretsRefreshTimer;
    private final Counter secretsSecretNotFoundCounter;
    private final Counter secretsLimitExceededCounter;
    private final Counter secretsAccessDeniedCounter;

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
        this.secretsSecretNotFoundCounter = pluginMetrics.counterWithTags(
                SECRETS_SECRET_NOT_FOUND, SECRET_CONFIG_ID_TAG, secretConfigId);
        this.secretsLimitExceededCounter = pluginMetrics.counterWithTags(
                SECRETS_LIMIT_EXCEEDED, SECRET_CONFIG_ID_TAG, secretConfigId);
        this.secretsAccessDeniedCounter = pluginMetrics.counterWithTags(
                SECRETS_ACCESS_DENIED, SECRET_CONFIG_ID_TAG, secretConfigId);
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
                    SecretsManagerException sme = (SecretsManagerException) e;
                    if (sme instanceof ResourceNotFoundException) {
                        secretsSecretNotFoundCounter.increment();
                    } else if (sme instanceof LimitExceededException) {
                        secretsLimitExceededCounter.increment();
                    } else if (sme.awsErrorDetails() != null && 
                               "AccessDeniedException".equals(sme.awsErrorDetails().errorCode())) {
                        secretsAccessDeniedCounter.increment();
                    }
                }
                LOG.error("Failed to refresh secrets in aws:secrets:{}.", secretConfigId, e);
                secretsRefreshFailureCounter.increment();
            }
        });
    }
}
