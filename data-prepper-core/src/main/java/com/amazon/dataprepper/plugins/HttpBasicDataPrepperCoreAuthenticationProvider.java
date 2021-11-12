/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.pipeline.server.DataPrepperCoreAuthenticationProvider;
import com.amazon.dataprepper.pipeline.server.HttpBasicAuthenticationConfig;
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
