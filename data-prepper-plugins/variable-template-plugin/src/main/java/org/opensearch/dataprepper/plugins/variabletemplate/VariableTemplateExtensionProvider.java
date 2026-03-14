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

import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

import java.util.Optional;

public class VariableTemplateExtensionProvider implements ExtensionProvider<PluginConfigValueTranslator> {

    private final PluginConfigValueTranslator translator;

    public VariableTemplateExtensionProvider(final PluginConfigValueTranslator translator) {
        this.translator = translator;
    }

    @Override
    public Optional<PluginConfigValueTranslator> provideInstance(final Context context) {
        return Optional.of(translator);
    }

    @Override
    public Class<PluginConfigValueTranslator> supportedClass() {
        return PluginConfigValueTranslator.class;
    }
}
