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

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.annotation.ConnectorExecutor;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Resolves and instantiates the correct {@link RemoteConnectorExecutor} for a given
 * {@link Connector} by matching the connector's protocol against the value of the
 * {@link ConnectorExecutor @ConnectorExecutor} annotation on each registered executor class.
 *
 * <p>New executor types are registered by adding their class to {@link #EXECUTOR_CLASSES}.
 * No other code needs to change — the protocol-to-class mapping is derived entirely from
 * the annotation at static initialisation time.
 */
public final class ConnectorExecutorFactory {

    /**
     * All {@link RemoteConnectorExecutor} implementations. Each must carry a
     * {@link ConnectorExecutor @ConnectorExecutor} annotation whose value is the protocol
     * string it handles (e.g. {@code "aws_sigv4"}).
     */
    private static final List<Class<? extends RemoteConnectorExecutor>> EXECUTOR_CLASSES = Arrays.asList(
            AwsConnectorExecutor.class
    );

    /**
     * Protocol → executor-class registry, built once from {@link #EXECUTOR_CLASSES}.
     */
    private static final Map<String, Class<? extends RemoteConnectorExecutor>> REGISTRY =
            EXECUTOR_CLASSES.stream()
                    .filter(cls -> cls.isAnnotationPresent(ConnectorExecutor.class))
                    .collect(Collectors.toMap(
                            cls -> cls.getAnnotation(ConnectorExecutor.class).value(),
                            cls -> cls
                    ));

    private ConnectorExecutorFactory() {}

    /**
     * Creates a {@link RemoteConnectorExecutor} for the given connector by looking up the
     * executor class whose {@link ConnectorExecutor @ConnectorExecutor} annotation value
     * matches {@code connector.getProtocol()}, then invoking its
     * {@code (Connector, MLProcessorConfig, AwsCredentialsSupplier)} constructor via reflection.
     *
     * @param connector the connector whose {@code protocol} field selects the executor
     * @param config    processor config (credentials, region, etc.)
     * @param supplier  AWS credential supplier
     * @return a new {@link RemoteConnectorExecutor} instance for the connector's protocol
     * @throws IllegalArgumentException if no executor is registered for the protocol
     * @throws RuntimeException         if the constructor cannot be invoked
     */
    public static RemoteConnectorExecutor create(final Connector connector,
                                                 final MLProcessorConfig config,
                                                 final AwsCredentialsSupplier supplier) {
        final String protocol = connector.getProtocol();
        final Class<? extends RemoteConnectorExecutor> executorClass = REGISTRY.get(protocol);
        if (executorClass == null) {
            throw new IllegalArgumentException("No connector executor registered for protocol: " + protocol);
        }
        try {
            final Constructor<? extends RemoteConnectorExecutor> constructor =
                    executorClass.getConstructor(Connector.class, MLProcessorConfig.class, AwsCredentialsSupplier.class);
            return constructor.newInstance(connector, config, supplier);
        } catch (final Exception e) {
            throw new RuntimeException(
                    "Failed to instantiate connector executor for protocol: " + protocol, e);
        }
    }
}
