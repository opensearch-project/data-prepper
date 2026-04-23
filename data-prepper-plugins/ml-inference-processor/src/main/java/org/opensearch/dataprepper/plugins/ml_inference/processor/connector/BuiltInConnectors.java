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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Registry of built-in connector definitions shipped with the ml_inference processor.
 *
 * <p>Each connector is stored as a JSON resource file under
 * {@code org/opensearch/dataprepper/plugins/ml_inference/connector/} and named
 * {@code <modelId>.json}. At lookup time the file is read from the classpath and
 * returned as a JSON string. When the processor config specifies a model ID that
 * matches a built-in entry, the JSON is parsed into a typed {@link Connector}
 * (e.g. {@link AwsConnector} for {@code aws_sigv4}), and the matching
 * {@link RemoteConnectorExecutor} is instantiated via {@link ConnectorExecutorFactory}
 * — without routing through an OpenSearch domain or the ml-commons plugin.
 *
 * <p>Currently registered connectors:
 * <ul>
 *   <li>{@value #TITAN_EMBED_V2_MODEL_ID} — Amazon Bedrock Titan Text Embeddings V2</li>
 *   <li>{@value #TITAN_MULTIMODAL_EMBED_V1_MODEL_ID} — Amazon Bedrock Titan Multi-modal Embeddings V1</li>
 * </ul>
 */
public final class BuiltInConnectors {

    private static final Logger LOG = LoggerFactory.getLogger(BuiltInConnectors.class);

    /**
     * Model ID for Amazon Bedrock Titan Text Embeddings V2.
     */
    public static final String TITAN_EMBED_V2_MODEL_ID = "amazon.titan-embed-text-v2:0";

    /**
     * Model ID for Amazon Bedrock Titan Multi-modal Embeddings V1.
     */
    public static final String TITAN_MULTIMODAL_EMBED_V1_MODEL_ID = "amazon.titan-embed-image-v1";

    private static final String RESOURCE_BASE = "org/opensearch/dataprepper/plugins/ml_inference/connector/";

    private BuiltInConnectors() {}

    /**
     * Returns the raw connector JSON string for the given model ID, if a built-in JSON
     * resource exists for it. The JSON's {@code protocol} field determines which
     * {@link Connector} subclass and {@link RemoteConnectorExecutor} will be used at runtime.
     *
     * @param modelId the model ID from the processor config (e.g. {@value #TITAN_EMBED_V2_MODEL_ID})
     * @return an {@link Optional} containing the connector JSON, or empty if not a built-in model
     */
    public static Optional<String> findConnectorJson(final String modelId) {
        if (modelId == null) {
            return Optional.empty();
        }
        final String resourcePath = RESOURCE_BASE + modelId + ".json";
        final InputStream stream = BuiltInConnectors.class.getClassLoader().getResourceAsStream(resourcePath);
        if (stream == null) {
            LOG.debug("No built-in connector found for model: {}", modelId);
            return Optional.empty();
        }
        try (stream) {
            final String json = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            LOG.info("Loaded built-in connector definition for model: {}", modelId);
            return Optional.of(json);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to load built-in connector definition for model: " + modelId, e);
        }
    }
}
