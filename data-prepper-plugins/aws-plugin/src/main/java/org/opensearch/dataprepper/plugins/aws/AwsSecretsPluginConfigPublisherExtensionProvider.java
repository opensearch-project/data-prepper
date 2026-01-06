/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
