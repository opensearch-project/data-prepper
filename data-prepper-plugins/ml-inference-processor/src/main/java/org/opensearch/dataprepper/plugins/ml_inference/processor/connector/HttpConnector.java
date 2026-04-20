/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.connector;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.opensearch.dataprepper.plugins.ml_inference.processor.annotation.Connector;

/**
 * Connector implementation for plain HTTP (unauthenticated) services.
 * Identified by {@code "protocol": "http"} in the connector JSON.
 * Also serves as the default implementation for unrecognised protocols.
 *
 * <p>All behaviour is inherited from {@link AbstractConnector}; this class exists as the
 * concrete counterpart to {@link AwsConnector} for non-AWS endpoints.
 */
@Connector(ConnectorProtocols.HTTP)
@JsonIgnoreProperties(ignoreUnknown = true)
public class HttpConnector extends AbstractConnector {

    public HttpConnector() {
        super();
    }
}
