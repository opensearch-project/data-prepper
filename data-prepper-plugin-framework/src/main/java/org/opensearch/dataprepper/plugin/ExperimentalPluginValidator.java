/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.model.plugin.NoPluginFoundException;

import javax.inject.Named;
import java.util.function.Consumer;

@Named
class ExperimentalPluginValidator implements Consumer<DefinedPlugin<?>> {
    private final ExperimentalConfiguration experimentalConfiguration;

    ExperimentalPluginValidator(final ExperimentalConfigurationContainer experimentalConfigurationContainer) {
        this.experimentalConfiguration = experimentalConfigurationContainer.getExperimental();
    }

    @Override
    public void accept(final DefinedPlugin<?> definedPlugin) {
        if(isPluginDisallowedAsExperimental(definedPlugin.getPluginClass())) {
            throw new NoPluginFoundException("Unable to create experimental plugin " + definedPlugin.getPluginName() +
                    ". You must enable experimental plugins in data-prepper-config.yaml in order to use them.");
        }
    }

    private boolean isPluginDisallowedAsExperimental(final Class<?> pluginClass) {
        return pluginClass.isAnnotationPresent(Experimental.class) && !experimentalConfiguration.isEnableAll();
    }
}
