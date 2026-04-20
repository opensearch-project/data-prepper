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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Abstract base implementation of {@link Connector}, analogous to {@code AbstractConnector}
 * in ml-commons.
 *
 * <p>Holds all shared connector state (protocol, parameters, actions) and provides the
 * common parameter-merge and template-substitution logic. Jackson uses the {@code protocol}
 * field to dispatch deserialization to the correct concrete subclass.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "protocol",
        visible = true,
        defaultImpl = HttpConnector.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = AwsConnector.class, name = ConnectorProtocols.AWS_SIGV4),
        @JsonSubTypes.Type(value = HttpConnector.class, name = ConnectorProtocols.HTTP)
})
public abstract class AbstractConnector implements Connector {

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @JsonProperty("name")
    private String name;

    @JsonProperty("version")
    private String version;

    @JsonProperty("description")
    private String description;

    @JsonProperty("protocol")
    private String protocol;

    @JsonProperty("parameters")
    private Map<String, String> parameters;

    @JsonProperty("actions")
    private List<ConnectorAction> actions;

    protected AbstractConnector() {}

    @Override
    public String getName() { return name; }

    @Override
    public String getProtocol() { return protocol; }

    @Override
    public Map<String, String> getParameters() {
        return parameters != null ? parameters : Collections.emptyMap();
    }

    @Override
    public List<ConnectorAction> getActions() {
        return actions != null ? actions : Collections.emptyList();
    }

    @Override
    public Optional<ConnectorAction> findAction(final String actionType) {
        if (actions == null || actionType == null) {
            return Optional.empty();
        }
        return actions.stream()
                .filter(a -> actionType.equalsIgnoreCase(a.getActionType()))
                .findFirst();
    }

    @Override
    public Map<String, String> mergeParameters(final Map<String, String> runtimeParameters) {
        final Map<String, String> merged = new HashMap<>(getParameters());
        if (runtimeParameters != null) {
            merged.putAll(runtimeParameters);
        }
        return merged;
    }

    @Override
    public String getActionEndpoint(final String actionType, final Map<String, String> mergedParameters) {
        return findAction(actionType)
                .map(a -> a.buildUrl(mergedParameters))
                .orElse(null);
    }

    @Override
    public String createPayload(final String actionType, final Map<String, String> mergedParameters) {
        return findAction(actionType)
                .map(a -> a.buildRequestBody(mergedParameters))
                .orElse("");
    }

    @Override
    public String getServiceName() {
        return getParameters().get("service_name");
    }

    /**
     * Deserializes a connector from JSON, returning the most-specific subclass based
     * on the {@code protocol} field.
     */
    public static AbstractConnector fromJson(final String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, AbstractConnector.class);
    }
}
