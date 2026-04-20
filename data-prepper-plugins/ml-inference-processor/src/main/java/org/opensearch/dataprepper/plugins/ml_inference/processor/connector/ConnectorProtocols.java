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

/**
 * Constants for supported connector protocols, analogous to {@code ConnectorProtocols} in ml-commons.
 */
public final class ConnectorProtocols {

    public static final String HTTP = "http";
    public static final String AWS_SIGV4 = "aws_sigv4";

    private ConnectorProtocols() {}
}
