/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ServiceName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MLBatchJobCreatorFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MLProcessor.class);

    // A thread-safe map to store singleton instances of each service
    private static final Map<ServiceName, MLBatchJobCreator> jobCreators = new ConcurrentHashMap<>();

    public static MLBatchJobCreator getJobCreator(final ServiceName serviceName, final MLProcessorConfig mlProcessorConfig,
                                                  final AwsCredentialsSupplier awsCredentialsSupplier, final PluginMetrics pluginMetrics) {
        // If the instance for the given service name is already created, return it
        if (serviceName == null) {
            throw new IllegalArgumentException("ServiceName cannot be null");
        }
        return jobCreators.computeIfAbsent(serviceName, key -> createJobCreator(key, mlProcessorConfig, awsCredentialsSupplier, pluginMetrics));
    }

    private static MLBatchJobCreator createJobCreator(final ServiceName serviceName, final MLProcessorConfig mlProcessorConfig,
                                                      final AwsCredentialsSupplier awsCredentialsSupplier, final PluginMetrics pluginMetrics) {
        switch (serviceName) {
            case SAGEMAKER:
                return new SageMakerBatchJobCreator(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics);
            case BEDROCK:
                return new BedrockBatchJobCreator(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics);
            default:
                LOG.warn("Unsupported service name provided: {}", serviceName);
                throw new IllegalArgumentException(
                        "Unsupported ServiceName: " + serviceName +
                                ". Expected values: SAGEMAKER, BEDROCK."
                );
        }
    }
}
