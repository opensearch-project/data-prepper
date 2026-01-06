/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.auth.AuthService;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * The plugin for HTTP Basic authentication of Armeria servers.
 *
 * @since 1.2
 */
@DataPrepperPlugin(name = "http_basic",
        pluginType = ArmeriaHttpAuthenticationProvider.class,
        pluginConfigurationType = HttpBasicAuthenticationConfig.class)
public class HttpBasicArmeriaHttpAuthenticationProvider implements ArmeriaHttpAuthenticationProvider {

    private final HttpBasicAuthenticationConfig httpBasicAuthenticationConfig;

    @DataPrepperPluginConstructor
    public HttpBasicArmeriaHttpAuthenticationProvider(final HttpBasicAuthenticationConfig httpBasicAuthenticationConfig) {
        Objects.requireNonNull(httpBasicAuthenticationConfig);
        Objects.requireNonNull(httpBasicAuthenticationConfig.getUsername());
        Objects.requireNonNull(httpBasicAuthenticationConfig.getPassword());
        this.httpBasicAuthenticationConfig = httpBasicAuthenticationConfig;
    }

    @Override
    public Optional<Function<? super HttpService, ? extends HttpService>> getAuthenticationDecorator() {
        return Optional.of(createDecorator());
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
