/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;

import java.util.Optional;

public class AwsSecretsPluginConfigPublisherExtensionProvider implements ExtensionProvider<PluginConfigPublisher> {

    private final PluginConfigPublisher pluginConfigPublisher;

    AwsSecretsPluginConfigPublisherExtensionProvider(final PluginConfigPublisher pluginConfigPublisher) {
        this.pluginConfigPublisher = pluginConfigPublisher;
    }

    @Override
    public Optional<PluginConfigPublisher> provideInstance(final Context context) {
        return Optional.ofNullable(pluginConfigPublisher);
    }

    @Override
    public Class<PluginConfigPublisher> supportedClass() {
        return PluginConfigPublisher.class;
    }
}
