/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collections;
import java.util.List;

@Named("extensionsApplier")
class ExtensionsApplier {
    private static final Logger LOG = LoggerFactory.getLogger(ExtensionsApplier.class);

    private final DataPrepperExtensionPoints dataPrepperExtensionPoints;
    private final ExtensionLoader extensionLoader;
    private List<? extends ExtensionPlugin> loadedExtensionPlugins = Collections.emptyList();

    @Inject
    ExtensionsApplier(
            final DataPrepperExtensionPoints dataPrepperExtensionPoints,
            final ExtensionLoader extensionLoader) {
        this.dataPrepperExtensionPoints = dataPrepperExtensionPoints;
        this.extensionLoader = extensionLoader;
    }

    @PostConstruct
    void applyExtensions() {
        loadedExtensionPlugins = extensionLoader.loadExtensions();

        LOG.info("Loaded {} extensions: {}", loadedExtensionPlugins.size(),  loadedExtensionPlugins);

        for (ExtensionPlugin extensionPlugin : loadedExtensionPlugins) {
            extensionPlugin.apply(dataPrepperExtensionPoints);
        }
    }

    @PreDestroy
    public void shutdownExtensions() {
        loadedExtensionPlugins.forEach(ExtensionPlugin::shutdown);
    }
}
