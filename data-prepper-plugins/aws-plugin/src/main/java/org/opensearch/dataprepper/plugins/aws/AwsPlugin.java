/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;

/**
 * The {@link ExtensionPlugin} class which adds the AWS Plugin to
 * Data Prepper as an extension plugin. Everything starts from here.
 */
@DataPrepperExtensionPlugin(modelType = AwsPluginConfig.class, rootKeyJsonPath = "/aws/configurations")
public class AwsPlugin implements ExtensionPlugin {
    private final DefaultAwsCredentialsSupplier defaultAwsCredentialsSupplier;

    private final AwsPluginConfig awsPluginConfig;

    @DataPrepperPluginConstructor
    public AwsPlugin(final AwsPluginConfig awsPluginConfig) {

        this.awsPluginConfig = awsPluginConfig;

        final CredentialsProviderFactory credentialsProviderFactory = new CredentialsProviderFactory(awsPluginConfig != null ? awsPluginConfig.getDefaultStsConfiguration() : new AwsStsConfiguration());
        final CredentialsCache credentialsCache = new CredentialsCache();
        defaultAwsCredentialsSupplier = new DefaultAwsCredentialsSupplier(credentialsProviderFactory, credentialsCache);
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        extensionPoints.addExtensionProvider(new AwsExtensionProvider(defaultAwsCredentialsSupplier));
    }
}
