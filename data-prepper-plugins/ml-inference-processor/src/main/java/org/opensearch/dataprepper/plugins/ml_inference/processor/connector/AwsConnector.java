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

import static org.opensearch.dataprepper.plugins.ml_inference.processor.connector.ConnectorProtocols.AWS_SIGV4;

/**
 * Connector for AWS SigV4-signed services (Bedrock, SageMaker, etc.), analogous to
 * {@code AwsConnector} in ml-commons.
 *
 * <p>Identified by {@code "protocol": "aws_sigv4"} in the connector JSON. All connector
 * state and logic are inherited from {@link HttpConnector} / {@link AbstractConnector};
 * this class exists so that the Jackson dispatcher returns a typed instance and callers
 * can route it to {@link AwsConnectorExecutor}.
 */
@Connector(AWS_SIGV4)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AwsConnector extends HttpConnector {

    public AwsConnector() {
        super();
    }
}
