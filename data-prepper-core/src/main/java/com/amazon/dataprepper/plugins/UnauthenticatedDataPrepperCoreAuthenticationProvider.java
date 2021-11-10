package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.pipeline.server.DataPrepperCoreAuthenticationProvider;
import com.sun.net.httpserver.Authenticator;

/**
 * The plugin for unauthenticated core Data Prepper APIs.
 *
 * @since 1.2
 */
@DataPrepperPlugin(name = DataPrepperCoreAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME,
        pluginType = DataPrepperCoreAuthenticationProvider.class)
public class UnauthenticatedDataPrepperCoreAuthenticationProvider implements DataPrepperCoreAuthenticationProvider {
    @Override
    public Authenticator getAuthenticator() {
        return null;
    }
}
