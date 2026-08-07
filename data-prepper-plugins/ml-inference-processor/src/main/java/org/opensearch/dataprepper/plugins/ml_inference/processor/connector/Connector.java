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

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Contract for remote-service connector definitions, analogous to the {@code Connector}
 * interface in ml-commons.
 *
 * <p>A connector bundles the protocol, default parameters, and a list of
 * {@link ConnectorAction}s. Implementations are selected at deserialization time based
 * on the {@code protocol} field in the connector JSON.
 */
public interface Connector {

    String getName();

    String getProtocol();

    Map<String, String> getParameters();

    List<ConnectorAction> getActions();

    Optional<ConnectorAction> findAction(String actionType);

    Map<String, String> mergeParameters(Map<String, String> runtimeParameters);

    String getActionEndpoint(String actionType, Map<String, String> mergedParameters);

    String createPayload(String actionType, Map<String, String> mergedParameters);

    String getServiceName();
}
