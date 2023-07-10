/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.handler;

import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;

import java.util.Optional;

public class BasicAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {
    @Override
    public Optional<HttpAuthOptions> authenticate(HttpSinkConfiguration sinkConfiguration) {
        // if ssl enabled then set connection manager
        return null;
    }
}
