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

import java.util.Map;

/**
 * Interface for executing actions defined by a {@link Connector} against a remote ML service,
 * analogous to {@code RemoteConnectorExecutor} in ml-commons.
 *
 * <p>Implementations are protocol-specific (e.g. {@link AwsConnectorExecutor} for
 * {@code aws_sigv4}) and are selected based on the connector's protocol at runtime.
 */
public interface RemoteConnectorExecutor {

    /**
     * Returns the connector definition this executor operates against.
     */
    Connector getConnector();

    /**
     * Executes the named action from the connector using the given runtime parameters.
     *
     * @param actionType        the action to execute (e.g. BATCH_PREDICT)
     * @param runtimeParameters per-request parameter overrides merged with connector defaults
     */
    void executeAction(ConnectorActionType actionType, Map<String, String> runtimeParameters);
}
