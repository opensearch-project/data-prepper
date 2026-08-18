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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Abstract base class for connector executors, analogous to {@code AbstractConnectorExecutor}
 * in ml-commons.
 *
 * <p>Resolves the URL and payload via template substitution, then delegates the actual
 * HTTP transport to {@link #sendRequest} in the concrete subclass.
 */
public abstract class AbstractConnectorExecutor implements RemoteConnectorExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractConnectorExecutor.class);

    @Override
    public void executeAction(final ConnectorActionType actionType,
                              final Map<String, String> runtimeParameters) {
        resolveAndSend(actionType, runtimeParameters);
    }

    @Override
    public String executeActionAndGetResponse(final ConnectorActionType actionType,
                                              final Map<String, String> runtimeParameters) {
        return resolveAndSend(actionType, runtimeParameters);
    }

    private String resolveAndSend(final ConnectorActionType actionType,
                                  final Map<String, String> runtimeParameters) {
        final Connector connector = getConnector();
        final String actionName = actionType.name();

        LOG.debug("Executing action '{}' on connector '{}'", actionName, connector.getName());

        final Map<String, String> merged = connector.mergeParameters(runtimeParameters);

        final ConnectorAction action = connector.findAction(actionName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Connector '" + connector.getName() + "' has no action for: " + actionName));

        final String url = connector.getActionEndpoint(actionName, merged);
        final String payload = connector.createPayload(actionName, merged);

        LOG.debug("Sending {} request to: {}", action.getMethod(), url);
        return sendRequest(action, url, payload, merged);
    }

    /**
     * Performs the actual HTTP request and returns the raw response body.
     *
     * @param action  the matching connector action (carries method, headers, etc.)
     * @param url     the fully-resolved request URL
     * @param payload the fully-resolved request body
     * @param merged  the merged parameter map (connector defaults + runtime overrides)
     * @return the raw HTTP response body string
     */
    protected abstract String sendRequest(ConnectorAction action,
                                          String url,
                                          String payload,
                                          Map<String, String> merged);
}
