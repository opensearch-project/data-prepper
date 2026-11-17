/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import org.opensearch.dataprepper.gcp.api.GcpAuthMode;
import org.opensearch.dataprepper.gcp.api.GcpConfig;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsOptions;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.ExtensionProvides;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;

/**
 * The {@link ExtensionPlugin} that provides {@link GcpCredentialsSupplier} to Data Prepper plugins.
 */
@DataPrepperExtensionPlugin(modelType = GcpPluginConfig.class, rootKeyJsonPath = "/gcp/configurations")
@ExtensionProvides(providedClasses = {GcpCredentialsSupplier.class})
public class GcpPlugin implements ExtensionPlugin {
    private final GcpCredentialsSupplier gcpCredentialsSupplier;

    @DataPrepperPluginConstructor
    public GcpPlugin(final GcpPluginConfig gcpPluginConfig) {
        this(gcpPluginConfig, GoogleCredentials::getApplicationDefault);
    }

    GcpPlugin(final GcpPluginConfig gcpPluginConfig, final ApplicationDefaultCredentialsLoader applicationDefaultCredentialsLoader) {
        final GcpConfig defaultConfig = gcpPluginConfig != null ? gcpPluginConfig.getDefaultConfig() : null;
        final GcpCredentialsOptions defaultOptions = defaultConfig != null ? defaultConfig.toCredentialsOptions() : null;
        gcpCredentialsSupplier = new DefaultGcpCredentialsSupplier(
                defaultOptions,
                new GoogleCredentialsFactory(applicationDefaultCredentialsLoader, new TokenUriValidator()),
                new CredentialsCache(),
                applicationDefaultCredentialsLoader);
        if (defaultOptions != null && defaultOptions.getAuthMode() == GcpAuthMode.SERVICE_ACCOUNT_KEY) {
            gcpCredentialsSupplier.getCredentials(defaultOptions);
        }
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        extensionPoints.addExtensionProvider(new GcpExtensionProvider(gcpCredentialsSupplier));
    }
}
