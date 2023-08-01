/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.handler;

/**
 * An interface to handle multiple authentications
 */
public interface MultiAuthPrometheusSinkHandler {

    /**
     * This method can be used to implement multiple authentication based on configuration
     * @param httpAuthOptionsBuilder HttpAuthOptions.Builder
     * @return HttpAuthOptions
     */
    HttpAuthOptions authenticate(final HttpAuthOptions.Builder  httpAuthOptionsBuilder);

}
