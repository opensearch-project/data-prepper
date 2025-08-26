/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.core.validation.PluginErrorCollector;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.validation.PluginError;
import org.opensearch.dataprepper.validation.PluginErrorsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Named("extensionsApplier")
class ExtensionsApplier {
    private static final Logger LOG = LoggerFactory.getLogger(ExtensionsApplier.class);

    private final DataPrepperExtensionPoints dataPrepperExtensionPoints;
    private final ExtensionLoader extensionLoader;
    private final PluginErrorCollector pluginErrorCollector;
    private final PluginErrorsHandler pluginErrorsHandler;
    private List<? extends ExtensionPlugin> loadedExtensionPlugins = Collections.emptyList();

    @Inject
    ExtensionsApplier(
            final DataPrepperExtensionPoints dataPrepperExtensionPoints,
            final ExtensionLoader extensionLoader,
            final PluginErrorCollector pluginErrorCollector,
            final PluginErrorsHandler pluginErrorsHandler) {
        this.dataPrepperExtensionPoints = dataPrepperExtensionPoints;
        this.extensionLoader = extensionLoader;
        this.pluginErrorCollector = pluginErrorCollector;
        this.pluginErrorsHandler = pluginErrorsHandler;
    }

    @PostConstruct
    void applyExtensions() {
        loadedExtensionPlugins = extensionLoader.loadExtensions();

        LOG.info("Loaded {} extensions: {}", loadedExtensionPlugins.size(),  loadedExtensionPlugins);

        for (ExtensionPlugin extensionPlugin : loadedExtensionPlugins) {
            try {
                extensionPlugin.apply(dataPrepperExtensionPoints);
            } catch (Exception e) {
                final PluginError pluginError = PluginError.builder()
                        .componentType(PipelinesDataFlowModel.EXTENSION_PLUGIN_TYPE)
                        .pluginName(extensionLoader.convertClassToName(extensionPlugin.getClass()))
                        .exception(e)
                        .build();
                pluginErrorCollector.collectPluginError(pluginError);
                LOG.error("Failed to apply extension plugin {}", extensionPlugin.getClass(), e);
            }
        }

        handlePluginErrors();
    }

    @PreDestroy
    public void shutdownExtensions() {
        loadedExtensionPlugins.forEach(ExtensionPlugin::shutdown);
    }

    private void handlePluginErrors() {
        final List<PluginError> extensionPluginErrors = pluginErrorCollector.getPluginErrors()
                .stream().filter(pluginError -> PipelinesDataFlowModel.EXTENSION_PLUGIN_TYPE
                        .equals(pluginError.getComponentType()))
                .collect(Collectors.toList());

        if (!extensionPluginErrors.isEmpty()) {
            pluginErrorsHandler.handleErrors(extensionPluginErrors);
            throw new RuntimeException(
                    "One or more extension plugins could not be applied correctly.");
        }
    }
}
