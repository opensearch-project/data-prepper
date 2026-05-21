/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.variabletemplate;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DataPrepperExtensionPlugin(
        modelType = VariableTemplatePluginConfig.class,
        rootKeyJsonPath = "/variable_sources",
        allowInPipelineConfigurations = false)
public class VariableTemplatePlugin implements ExtensionPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(VariableTemplatePlugin.class);

    private final VariableTemplatePluginConfig config;

    @DataPrepperPluginConstructor
    public VariableTemplatePlugin(final VariableTemplatePluginConfig config) {
        this.config = config;
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        if (config == null) {
            return;
        }

        final VariableTemplatePluginConfig.Resolvers resolvers = config.getResolvers();

        if (resolvers.isEnvEnabled()) {
            LOG.info("VariableTemplatePlugin: registering env resolver.");
            extensionPoints.addExtensionProvider(
                    new VariableTemplateExtensionProvider(new EnvVariableTranslator()));
        }

        if (resolvers.isFileEnabled()) {
            LOG.info("VariableTemplatePlugin: registering file resolver.");
            extensionPoints.addExtensionProvider(
                    new VariableTemplateExtensionProvider(new FileVariableTranslator()));
        }

        final VariableTemplatePluginConfig.StoreResolverConfig storeConfig = resolvers.getStore();
        if (storeConfig != null && storeConfig.isEnabled()) {
            LOG.info("VariableTemplatePlugin: registering store resolver with {} source(s).", storeConfig.getSources().size());
            extensionPoints.addExtensionProvider(
                    new VariableTemplateExtensionProvider(new StoreVariableTranslator(storeConfig.getSources())));
        }
    }

    @Override
    public void shutdown() {
    }
}
