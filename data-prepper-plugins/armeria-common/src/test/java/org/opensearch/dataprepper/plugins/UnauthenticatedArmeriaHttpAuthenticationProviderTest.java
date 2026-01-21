/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.auth.BasicToken;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class UnauthenticatedArmeriaHttpAuthenticationProviderTest {
    @RegisterExtension
    static ServerExtension server = new ServerExtension() {
        @Override
        protected void configure(final ServerBuilder sb) {
            sb.service("/test", (ctx, req) -> HttpResponse.of(200));
            new UnauthenticatedArmeriaHttpAuthenticationProvider().getAuthenticationDecorator();
        }
    };

    @Test
    void httpRequest_without_authentication_responds_OK() {
        final WebClient client = WebClient.of(server.httpUri());

        final AggregatedHttpResponse httpResponse = client.get("/test").aggregate().join();

        assertThat(httpResponse.status(), equalTo(HttpStatus.OK));
    }

    @Test
    void httpRequest_with_BasicAuthentication_responds_OK() {
        final WebClient client = WebClient.builder(server.httpUri())
                .auth(BasicToken.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build();

        final AggregatedHttpResponse httpResponse = client.get("/test").aggregate().join();

        assertThat(httpResponse.status(), equalTo(HttpStatus.OK));
    }
}