/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.encryption.EncryptionHttpHandler;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@DataPrepperExtensionPlugin(modelType = EncryptionPluginConfig.class, rootKeyJsonPath = "/encryption")
public class EncryptionPlugin implements ExtensionPlugin {
    static final int PERIOD_IN_SECONDS = 60;
    private static final Logger LOG = LoggerFactory.getLogger(EncryptionPlugin.class);

    private final EncryptionSupplier encryptionSupplier;
    private final EncryptionHttpHandler encryptionHttpHandler;
    private ScheduledExecutorService scheduledExecutorService;
    private PluginMetrics pluginMetrics;

    @DataPrepperPluginConstructor
    public EncryptionPlugin(final EncryptionPluginConfig encryptionPluginConfig) {
        final KeyProviderFactory keyProviderFactory = KeyProviderFactory.create();
        final EncryptionEngineFactory encryptionEngineFactory = EncryptionEngineFactory.create(keyProviderFactory);
        final EncryptedDataKeySupplierFactory encryptedDataKeySupplierFactory =
                EncryptedDataKeySupplierFactory.create();
        if (encryptionPluginConfig != null) {
            encryptionSupplier = new EncryptionSupplier(
                    encryptionPluginConfig, encryptionEngineFactory, encryptedDataKeySupplierFactory);
            pluginMetrics = PluginMetrics.fromPrefix("encryption");
            final EncryptedDataKeyWriterFactory encryptedDataKeyWriterFactory = new EncryptedDataKeyWriterFactory();
            final EncryptionRotationHandlerFactory encryptionRotationHandlerFactory =
                    EncryptionRotationHandlerFactory.create(pluginMetrics, encryptedDataKeyWriterFactory);
            final Set<EncryptionRotationHandler> encryptionRotationHandlers = encryptionPluginConfig
                    .getEncryptionConfigurationMap().entrySet().stream()
                    .filter(entry -> entry.getValue().rotationEnabled())
                    .map(entry -> encryptionRotationHandlerFactory
                            .createEncryptionRotationHandler(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toSet());
            encryptionHttpHandler = DefaultEncryptionHttpHandler.create(encryptionRotationHandlers);
            submitEncryptionRefreshJobs(encryptionPluginConfig, encryptionSupplier);
        } else {
            encryptionSupplier = new EncryptionSupplier(
                    new EncryptionPluginConfig(), encryptionEngineFactory, encryptedDataKeySupplierFactory);
            encryptionHttpHandler = DefaultEncryptionHttpHandler.create(Collections.emptySet());
        }
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        extensionPoints.addExtensionProvider(new EncryptionSupplierExtensionProvider(encryptionSupplier));
        extensionPoints.addExtensionProvider(new EncryptionHttpHandlerExtensionProvider(encryptionHttpHandler));
    }

    @Override
    public void shutdown() {
        if (scheduledExecutorService != null) {
            LOG.info("Shutting down secrets refreshing tasks.");
            scheduledExecutorService.shutdown();
            try {
                if (!scheduledExecutorService.awaitTermination(PERIOD_IN_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn("Secrets refreshing tasks did not terminate in time, forcing termination");
                    scheduledExecutorService.shutdownNow();
                }
            } catch (InterruptedException ex) {
                LOG.info("Encountered interruption terminating the secrets refreshing tasks execution, " +
                        "attempting to force the termination");
                scheduledExecutorService.shutdownNow();
            }
        }
    }

    private void submitEncryptionRefreshJobs(final EncryptionPluginConfig encryptionPluginConfig,
                                             final EncryptionSupplier encryptionSupplier) {
        final Map<String, EncryptionEngineConfiguration> encryptionEngineConfigurationMap = encryptionPluginConfig
                .getEncryptionConfigurationMap();
        final Map<String, EncryptedDataKeySupplier> rotationEnabledEncryptionIdToEncryptedDataSuppliers =
                encryptionEngineConfigurationMap.entrySet().stream()
                        .filter(entry -> entry.getValue().rotationEnabled())
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> encryptionSupplier.getEncryptedDataKeySupplier(entry.getKey())
                        ));
        if (!rotationEnabledEncryptionIdToEncryptedDataSuppliers.isEmpty()) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            rotationEnabledEncryptionIdToEncryptedDataSuppliers.forEach((encryptionId, encryptedDataKeySupplier) -> {
                final EncryptionEngineConfiguration encryptionEngineConfiguration = encryptionEngineConfigurationMap.get(encryptionId);
                final long jitterDelay = ThreadLocalRandom.current().nextLong(60L);
                scheduledExecutorService.scheduleAtFixedRate(
                        new EncryptionRefreshJob(encryptionId, encryptedDataKeySupplier, pluginMetrics),
                        jitterDelay,
                        encryptionEngineConfiguration.getRotationInterval().toSeconds(),
                        TimeUnit.SECONDS
                );
            });
        }
    }
}
