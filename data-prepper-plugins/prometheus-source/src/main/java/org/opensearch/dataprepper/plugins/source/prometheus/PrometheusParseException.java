/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

/**
 * Exception thrown when parsing Prometheus Remote Write protobuf data fails.
 */
public class PrometheusParseException extends Exception {

    public PrometheusParseException(final String message) {
        super(message);
    }

    public PrometheusParseException(final String message, final Throwable cause) {
        super(message, cause);
    }
}