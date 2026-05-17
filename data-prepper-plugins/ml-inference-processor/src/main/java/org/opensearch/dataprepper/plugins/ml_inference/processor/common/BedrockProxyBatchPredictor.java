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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.dataprepper.common.utils.RetryUtil;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.util.MlCommonRequester;

import static org.opensearch.dataprepper.common.utils.RetryUtil.retryWithBackoffWithResult;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.LOG;

/**
 * Submits a BATCH_PREDICT request through the ml-commons proxy.
 */
class BedrockProxyBatchPredictor implements BatchPredictor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String BEDROCK_PAYLOAD_TEMPLATE =
            "{\"parameters\": {\"inputDataConfig\": {\"s3InputDataConfig\": {\"s3Uri\": \"s3://\"}}," +
            "\"jobName\": \"\", \"outputDataConfig\": {\"s3OutputDataConfig\": {\"s3Uri\": \"s3://\"}}}}";

    private final MlCommonRequester mlCommonRequester;
    private final MLProcessorConfig mlProcessorConfig;
    private final MLBatchJobCreator jobNameSource;

    BedrockProxyBatchPredictor(final MlCommonRequester mlCommonRequester,
                            final MLProcessorConfig mlProcessorConfig,
                            final MLBatchJobCreator jobNameSource) {
        this.mlCommonRequester = mlCommonRequester;
        this.mlProcessorConfig = mlProcessorConfig;
        this.jobNameSource = jobNameSource;
    }

    @Override
    public RetryUtil.RetryResult predict(final String s3Uri) {
        LOG.debug("Submitting BATCH_PREDICT via ml-commons for: {}", s3Uri);
        final String payload = createPayloadBedrock(s3Uri);
        return retryWithBackoffWithResult(
                () -> mlCommonRequester.sendRequestToMLCommons(payload),
                LOG
        );
    }

    private String createPayloadBedrock(final String s3Uri) {
        if (s3Uri == null || s3Uri.isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid S3Uri: S3Uri is either null or empty. Please ensure the correct input S3 uris are provided");
        }

        try {
            final JsonNode rootNode = OBJECT_MAPPER.readTree(BEDROCK_PAYLOAD_TEMPLATE);
            ((ObjectNode) rootNode.at("/parameters/inputDataConfig/s3InputDataConfig")).put("s3Uri", s3Uri);
            ((ObjectNode) rootNode.at("/parameters")).put("jobName", jobNameSource.generateJobName());
            ((ObjectNode) rootNode.at("/parameters/outputDataConfig/s3OutputDataConfig")).put("s3Uri", mlProcessorConfig.getOutputPath());
            return OBJECT_MAPPER.writeValueAsString(rootNode);
        } catch (final Exception e) {
            LOG.error("Failed to create BedRock batch job payload with input {}.", s3Uri, e);
            throw new RuntimeException("Failed to create payload for BedRock batch job", e);
        }
    }
}
