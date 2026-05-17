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

import java.util.Collections;
import java.util.Map;

/**
 * Represents a single action (e.g. PREDICT, BATCH_PREDICT) defined in a connector.
 *
 * <p>URL and request body templates use {@code ${parameters.xxx}} placeholders that are
 * substituted at execution time from a merged parameters map — matching the ml-commons
 * connector action template convention.
 *
 * <p>Instances are typically deserialized from the connector's JSON definition via Jackson.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorAction {

    @JsonProperty("action_type")
    private String actionType;

    @JsonProperty("method")
    private String method;

    @JsonProperty("url")
    private String url;

    @JsonProperty("headers")
    private Map<String, String> headers;

    @JsonProperty("request_body")
    private String requestBody;

    @JsonProperty("pre_process_function")
    private String preProcessFunction;

    @JsonProperty("post_process_function")
    private String postProcessFunction;

    // Required by Jackson
    public ConnectorAction() {}

    public String getActionType() {
        return actionType;
    }

    public String getMethod() {
        return method;
    }

    public Map<String, String> getHeaders() {
        return headers != null ? headers : Collections.emptyMap();
    }

    public String getPreProcessFunction() {
        return preProcessFunction;
    }

    public String getPostProcessFunction() {
        return postProcessFunction;
    }

    /**
     * Builds the request URL by substituting {@code ${parameters.xxx}} placeholders
     * with values from the merged parameters map.
     */
    public String buildUrl(final Map<String, String> parameters) {
        return substituteParameters(url, parameters);
    }

    /**
     * Builds the JSON request body by substituting {@code ${parameters.xxx}} placeholders.
     * Returns an empty string when the template is empty (e.g. for GET requests).
     */
    public String buildRequestBody(final Map<String, String> parameters) {
        if (requestBody == null || requestBody.isEmpty()) {
            return "";
        }
        return substituteParameters(requestBody, parameters);
    }

    /**
     * Replaces every {@code ${parameters.<key>}} occurrence in {@code template} with the
     * corresponding value from {@code parameters}. Raw-object values (e.g. JSON snippets
     * for {@code inputDataConfig}) are substituted without extra quoting — it is the
     * caller's responsibility to put the correct string representation in the map.
     */
    private static String substituteParameters(final String template, final Map<String, String> parameters) {
        if (template == null) {
            return "";
        }
        String result = template;
        for (final Map.Entry<String, String> entry : parameters.entrySet()) {
            if (entry.getValue() != null) {
                result = result.replace("${parameters." + entry.getKey() + "}", entry.getValue());
            }
        }
        return result;
    }
}
