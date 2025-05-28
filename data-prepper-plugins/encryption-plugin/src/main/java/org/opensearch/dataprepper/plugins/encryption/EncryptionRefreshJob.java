/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EncryptionRefreshJob implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(EncryptionRefreshJob.class);
    static final String ENCRYPTION_REFRESH_SUCCESS = "encryptionRefreshSuccess";
    static final String ENCRYPTION_REFRESH_FAILURE = "encryptionRefreshFailure";
    static final String ENCRYPTION_REFRESH_DURATION = "encryptionRefreshDuration";
    static final String ENCRYPTION_ID_TAG = "encryptionId";

    private final String encryptionId;
    private final EncryptedDataKeySupplier encryptedDataKeySupplier;
    private final PluginMetrics pluginMetrics;
    private final Counter encryptionRefreshSuccessCounter;
    private final Counter encryptionRefreshFailureCounter;
    private final Timer encryptionRefreshTimer;

    public EncryptionRefreshJob(final String encryptionId,
                                final EncryptedDataKeySupplier encryptedDataKeySupplier,
                                final PluginMetrics pluginMetrics) {
        this.encryptionId = encryptionId;
        this.encryptedDataKeySupplier = encryptedDataKeySupplier;
        this.pluginMetrics = pluginMetrics;
        this.encryptionRefreshSuccessCounter = pluginMetrics.counterWithTags(
                ENCRYPTION_REFRESH_SUCCESS, ENCRYPTION_ID_TAG, encryptionId);
        this.encryptionRefreshFailureCounter = pluginMetrics.counterWithTags(
                ENCRYPTION_REFRESH_FAILURE, ENCRYPTION_ID_TAG, encryptionId);
        this.encryptionRefreshTimer = pluginMetrics.timerWithTags(
                ENCRYPTION_REFRESH_DURATION, ENCRYPTION_ID_TAG, encryptionId);
    }

    @Override
    public void run() {
        encryptionRefreshTimer.record(() -> {
            try {
                encryptedDataKeySupplier.refresh();
                encryptionRefreshSuccessCounter.increment();
            } catch(Exception e) {
                LOG.error("Failed to retrieve latest encrypted data key in encryption: {}.", encryptionId, e);
                encryptionRefreshFailureCounter.increment();
            }
        });
    }
}
