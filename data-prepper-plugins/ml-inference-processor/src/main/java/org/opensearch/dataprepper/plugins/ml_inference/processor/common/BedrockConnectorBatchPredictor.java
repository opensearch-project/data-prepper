/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.common.utils.RetryUtil;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.AbstractConnector;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.BuiltInConnectors;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.Connector;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.ConnectorActionType;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.ConnectorExecutorFactory;
import org.opensearch.dataprepper.plugins.ml_inference.processor.connector.RemoteConnectorExecutor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.dataprepper.common.utils.RetryUtil.retryWithBackoffWithResult;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.LOG;

/**
 * Submits a BATCH_PREDICT request directly to the model provider (e.g. Bedrock) via a
 * built-in {@link RemoteConnectorExecutor}, bypassing ml-commons.
 */
class BedrockConnectorBatchPredictor implements BatchPredictor {
    private static final com.fasterxml.jackson.databind.ObjectMapper OBJECT_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private final RemoteConnectorExecutor connectorExecutor;
    private final MLProcessorConfig mlProcessorConfig;
    private final MLBatchJobCreator jobNameSource;

    BedrockConnectorBatchPredictor(final RemoteConnectorExecutor connectorExecutor,
                           final MLProcessorConfig mlProcessorConfig,
                           final MLBatchJobCreator jobNameSource) {
        this.connectorExecutor = connectorExecutor;
        this.mlProcessorConfig = mlProcessorConfig;
        this.jobNameSource = jobNameSource;
    }

    /**
     * Attempts to build an {@link BedrockConnectorBatchPredictor} for the given model. Returns empty if no
     * built-in connector exists for the model, signalling that the ml-commons path should be used.
     */
    static Optional<BedrockConnectorBatchPredictor> create(final MLProcessorConfig config,
                                                   final AwsCredentialsSupplier supplier,
                                                   final MLBatchJobCreator jobNameSource) {
        return BuiltInConnectors.findConnectorJson(config.getModelId())
                .map(json -> {
                    try {
                        final Connector connector = AbstractConnector.fromJson(json);
                        final RemoteConnectorExecutor executor = ConnectorExecutorFactory.create(connector, config, supplier);
                        LOG.info("Using built-in connector for model: {}", config.getModelId());
                        return new BedrockConnectorBatchPredictor(executor, config, jobNameSource);
                    } catch (final Exception e) {
                        throw new RuntimeException("Failed to initialize connector for model: " + config.getModelId(), e);
                    }
                });
    }

    @Override
    public RetryUtil.RetryResult predict(final String s3Uri) {
        LOG.debug("Submitting BATCH_PREDICT via built-in connector for: {}", s3Uri);
        final Map<String, String> parameters = buildBatchPredictParameters(s3Uri);
        return retryWithBackoffWithResult(
                () -> connectorExecutor.executeAction(ConnectorActionType.BATCH_PREDICT, parameters),
                LOG
        );
    }

    private Map<String, String> buildBatchPredictParameters(final String s3InputUri) {
        try {
            final ObjectNode inputConfig = OBJECT_MAPPER.createObjectNode();
            inputConfig.set("s3InputDataConfig", OBJECT_MAPPER.createObjectNode().put("s3Uri", s3InputUri));

            final ObjectNode outputConfig = OBJECT_MAPPER.createObjectNode();
            outputConfig.set("s3OutputDataConfig", OBJECT_MAPPER.createObjectNode().put("s3Uri", mlProcessorConfig.getOutputPath()));

            final Map<String, String> parameters = new HashMap<>();
            parameters.put("region", mlProcessorConfig.getAwsAuthenticationOptions().getAwsRegion().id());
            parameters.put("jobName", jobNameSource.generateJobName());
            parameters.put("roleArn", mlProcessorConfig.getAwsAuthenticationOptions().getJobStsRoleArn());
            parameters.put("inputDataConfig", OBJECT_MAPPER.writeValueAsString(inputConfig));
            parameters.put("outputDataConfig", OBJECT_MAPPER.writeValueAsString(outputConfig));
            return parameters;
        } catch (final Exception e) {
            LOG.error("Failed to build Bedrock batch predict parameters for S3Uri: {}", s3InputUri, e);
            throw new RuntimeException("Failed to build Bedrock batch predict parameters", e);
        }
    }
}
