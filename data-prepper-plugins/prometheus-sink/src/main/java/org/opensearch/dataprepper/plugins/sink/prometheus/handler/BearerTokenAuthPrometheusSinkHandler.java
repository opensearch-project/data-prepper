/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.opensearch.dataprepper.plugins.sink.prometheus.OAuthAccessTokenManager;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.BearerTokenOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * * This class handles Bearer Token Authentication
 */
public class BearerTokenAuthPrometheusSinkHandler implements MultiAuthPrometheusSinkHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BearerTokenAuthPrometheusSinkHandler.class);

    private final HttpClientConnectionManager httpClientConnectionManager;

    private final BearerTokenOptions bearerTokenOptions;

    private final ObjectMapper objectMapper;

    private OAuthAccessTokenManager oAuthAccessTokenManager;

    public BearerTokenAuthPrometheusSinkHandler(final BearerTokenOptions bearerTokenOptions,
                                          final HttpClientConnectionManager httpClientConnectionManager,
                                          final OAuthAccessTokenManager oAuthAccessTokenManager){
        this.bearerTokenOptions = bearerTokenOptions;
        this.httpClientConnectionManager = httpClientConnectionManager;
        this.objectMapper = new ObjectMapper();
        this.oAuthAccessTokenManager = oAuthAccessTokenManager;
    }

    @Override
    public HttpAuthOptions authenticate(final HttpAuthOptions.Builder httpAuthOptionsBuilder) {
        //TODO: implementation
        return null;
    }
}
