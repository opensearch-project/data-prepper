/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.ExtensionProvides;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;

/**
 * The {@link ExtensionPlugin} class which adds the AWS Plugin to
 * Data Prepper as an extension plugin. Everything starts from here.
 */
@DataPrepperExtensionPlugin(modelType = AwsPluginConfig.class, rootKeyJsonPath = "/aws/configurations")
@ExtensionProvides(providedClasses = {AwsCredentialsSupplier.class})
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
