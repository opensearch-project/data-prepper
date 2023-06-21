/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.handler;

import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;

import java.util.Optional;

public class SecuredAuthHttpSinkHandler implements MultiAuthHttpSinkHandler {
    @Override
    public Optional<HttpAuthOptions> authenticate(HttpSinkConfiguration sinkConfiguration) {
        // logic here to read the certs from ACM/S3/local
        // SSL Sigv4 validation and verification and make connection
        return null;
    }
}
