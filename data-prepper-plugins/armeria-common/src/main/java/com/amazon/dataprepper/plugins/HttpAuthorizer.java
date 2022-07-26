/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import com.google.common.annotations.VisibleForTesting;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.auth.Authorizer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

public class HttpAuthorizer implements Authorizer<HttpRequest> {
    private final HttpBasicAuthenticationConfig httpBasicAuthenticationConfig;
    private final Pattern splitPattern = Pattern.compile(":");

    public HttpAuthorizer(final HttpBasicAuthenticationConfig httpBasicAuthenticationConfig) {
        this.httpBasicAuthenticationConfig = httpBasicAuthenticationConfig;
    }

    @Override
    public CompletionStage<Boolean> authorize(final ServiceRequestContext ctx, final HttpRequest data) {
        final boolean isHealthEndpoint = ctx.mappedPath().toLowerCase(Locale.ROOT).endsWith("health");

        if(isHealthEndpoint) {
            return CompletableFuture.completedFuture(true);
        }

        final String encodedAuth = data.headers().get("Authorization");
        final boolean isBasicAuth = encodedAuth.toLowerCase(Locale.ROOT).startsWith("basic ");

        if(isBasicAuth) {
            return CompletableFuture.completedFuture(authenticate(encodedAuth));
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    @VisibleForTesting
    protected boolean authenticate(final String authorization) {
        final String subEncodedAuth = authorization.substring("basic ".length());
        final String decodedAuth = new String(Base64.getDecoder().decode(subEncodedAuth), StandardCharsets.UTF_8);
        final String[] usernamePassword = splitPattern.split(decodedAuth);
        final String username = usernamePassword[0];
        final String password = usernamePassword[1];
        final boolean authenticated = Objects.equals(username, httpBasicAuthenticationConfig.getUsername())
                && Objects.equals(password, httpBasicAuthenticationConfig.getPassword());

        return authenticated;
    }
}
