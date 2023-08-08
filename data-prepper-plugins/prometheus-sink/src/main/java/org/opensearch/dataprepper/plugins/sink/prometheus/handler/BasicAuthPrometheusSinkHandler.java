/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.handler;

import org.apache.hc.client5.http.io.HttpClientConnectionManager;

/**
 * * This class handles Basic Authentication
 */
public class BasicAuthPrometheusSinkHandler implements MultiAuthPrometheusSinkHandler {

    private final HttpClientConnectionManager httpClientConnectionManager;

    private final String username;

    private final String password;

    public BasicAuthPrometheusSinkHandler(final String username,
                                    final String password,
                                    final HttpClientConnectionManager httpClientConnectionManager){
        this.httpClientConnectionManager = httpClientConnectionManager;
        this.username = username;
        this.password = password;
    }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions.Builder  httpAuthOptionsBuilder) {
        //TODO: implementation
        return null;
    }
}
