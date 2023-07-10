/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.handler;

import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;

import java.util.Optional;

public interface MultiAuthHttpSinkHandler {
    Optional<HttpAuthOptions> authenticate(final HttpSinkConfiguration sinkConfiguration);

}
