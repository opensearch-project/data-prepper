/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

import java.util.Optional;

public class AwsSecretsPluginConfigValueTranslatorExtensionProvider implements ExtensionProvider<PluginConfigValueTranslator> {
    private final PluginConfigValueTranslator pluginConfigValueTranslator;

    AwsSecretsPluginConfigValueTranslatorExtensionProvider(final PluginConfigValueTranslator pluginConfigValueTranslator) {
        this.pluginConfigValueTranslator = pluginConfigValueTranslator;
    }
    @Override
    public Optional<PluginConfigValueTranslator> provideInstance(Context context) {
        return Optional.ofNullable(pluginConfigValueTranslator);
    }

    @Override
    public Class<PluginConfigValueTranslator> supportedClass() {
        return PluginConfigValueTranslator.class;
    }
}
