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

import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
 * <p>To register a new model, drop a {@code <modelId>.json} file into the
 * {@code src/main/resources/org/opensearch/dataprepper/plugins/ml_inference/connector/}
 * directory — no code changes required.
 */
public final class BuiltInConnectors {

    private static final Logger LOG = LoggerFactory.getLogger(BuiltInConnectors.class);

    static final String RESOURCE_BASE = "org/opensearch/dataprepper/plugins/ml_inference/connector";
    private static final Pattern JSON_PATTERN = Pattern.compile(".*\\.json");

    private BuiltInConnectors() {}

    /**
     * Returns all model IDs for which a built-in connector JSON exists in the resource directory.
     * The list is derived dynamically from the classpath, so adding a new {@code <modelId>.json}
     * file is sufficient to register a new model — no constant or code change is needed.
     *
     * @return an unmodifiable list of known built-in model IDs
     */
    public static List<String> listBuiltInModelIds() {
        final Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forResource(RESOURCE_BASE))
                .setScanners(Scanners.Resources));
        return reflections.getResources(JSON_PATTERN).stream()
                .map(path -> path.substring(path.lastIndexOf('/') + 1))
                .map(name -> name.substring(0, name.length() - ".json".length()))
                .sorted()
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Returns the raw connector JSON string for the given model ID, if a built-in JSON
     * resource exists for it. The JSON's {@code protocol} field determines which
     * {@link Connector} subclass and {@link RemoteConnectorExecutor} will be used at runtime.
     *
     * @param modelId the model ID from the processor config
     * @return an {@link Optional} containing the connector JSON, or empty if not a built-in model
     */
    public static Optional<String> findConnectorJson(final String modelId) {
        if (modelId == null) {
            return Optional.empty();
        }
        final String resourcePath = RESOURCE_BASE + "/" + modelId + ".json";
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
