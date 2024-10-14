/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.core.pipeline.server.DataPrepperCoreAuthenticationProvider;
import org.opensearch.dataprepper.core.pipeline.server.HttpBasicAuthenticationConfig;
import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.BasicAuthenticator;

import java.util.Objects;

/**
 * The plugin for HTTP Basic authentication of the core Data Prepper APIs.
 *
 * @since 1.2
 */
@DataPrepperPlugin(name = "http_basic",
        pluginType = DataPrepperCoreAuthenticationProvider.class,
        pluginConfigurationType = HttpBasicAuthenticationConfig.class)
public class HttpBasicDataPrepperCoreAuthenticationProvider implements DataPrepperCoreAuthenticationProvider {

    private final HttpBasicAuthenticationConfig httpBasicAuthenticationConfig;
    private final CoreAuthenticator coreAuthenticator;

    @DataPrepperPluginConstructor
    public HttpBasicDataPrepperCoreAuthenticationProvider(final HttpBasicAuthenticationConfig httpBasicAuthenticationConfig) {
        this.httpBasicAuthenticationConfig = httpBasicAuthenticationConfig;
        Objects.requireNonNull(this.httpBasicAuthenticationConfig);
        Objects.requireNonNull(httpBasicAuthenticationConfig.getUsername());
        Objects.requireNonNull(httpBasicAuthenticationConfig.getPassword());

        coreAuthenticator = new CoreAuthenticator();
    }

    @Override
    public Authenticator getAuthenticator() {
        return coreAuthenticator;
    }

    private class CoreAuthenticator extends BasicAuthenticator {

        CoreAuthenticator() {
            super("core");
        }

        @Override
        public boolean checkCredentials(final String username, final String password) {
            return httpBasicAuthenticationConfig.getUsername().equals(username) &&
                    httpBasicAuthenticationConfig.getPassword().equals(password);
        }
    }
}
