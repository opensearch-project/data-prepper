package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.armeria.authentication.ArmeriaAuthenticationProvider;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.linecorp.armeria.server.ServerBuilder;

/**
 * The plugin to use for unauthenticated access to Armeria servers. It
 * disables authentication on endpoints.
 *
 * @since 1.2
 */
@DataPrepperPlugin(name = ArmeriaAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME, pluginType = ArmeriaAuthenticationProvider.class)
public class UnauthenticatedArmeriaAuthenticationProvider implements ArmeriaAuthenticationProvider {
    @Override
    public void addAuthenticationDecorator(final ServerBuilder serverBuilder) {
    }
}
