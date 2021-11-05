package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.armeria.authentication.ArmeriaAuthenticationProvider;
import com.amazon.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.auth.AuthService;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * The plugin for HTTP Basic authentication of Armeria servers.
 *
 * @since 1.2
 */
@DataPrepperPlugin(name = "http_basic",
        pluginType = ArmeriaAuthenticationProvider.class,
        pluginConfigurationType = HttpBasicAuthenticationConfig.class)
public class HttpBasicArmeriaAuthenticationProvider implements ArmeriaAuthenticationProvider {

    private final HttpBasicAuthenticationConfig httpBasicAuthenticationConfig;

    @DataPrepperPluginConstructor
    public HttpBasicArmeriaAuthenticationProvider(final HttpBasicAuthenticationConfig httpBasicAuthenticationConfig) {
        Objects.requireNonNull(httpBasicAuthenticationConfig);
        Objects.requireNonNull(httpBasicAuthenticationConfig.getUsername());
        Objects.requireNonNull(httpBasicAuthenticationConfig.getPassword());
        this.httpBasicAuthenticationConfig = httpBasicAuthenticationConfig;
    }

    @Override
    public void addAuthenticationDecorator(final ServerBuilder serverBuilder) {
        serverBuilder.decorator(createDecorator());
    }

    private Function<? super HttpService, ? extends HttpService> createDecorator() {
        return AuthService.builder()
                .addBasicAuth((context, basic) ->
                        CompletableFuture.completedFuture(
                                httpBasicAuthenticationConfig.getUsername().equals(basic.username()) &&
                                        httpBasicAuthenticationConfig.getPassword().equals(basic.password())))
                .newDecorator();
    }
}
