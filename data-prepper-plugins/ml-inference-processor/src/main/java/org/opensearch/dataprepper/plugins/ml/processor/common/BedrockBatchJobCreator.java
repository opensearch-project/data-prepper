/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml.processor.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml.processor.MLProcessorConfig;

import java.io.IOException;
import java.util.Collection;

import static org.opensearch.dataprepper.plugins.ml.processor.MLProcessor.LOG;
import static org.opensearch.dataprepper.plugins.ml.processor.util.MlCommonRequester.sendRequestToMLCommons;
import static org.opensearch.dataprepper.common.utils.RetryUtil.retryWithBackoff;

public class BedrockBatchJobCreator extends AbstractBatchJobCreator {
    private final MLProcessorConfig mlProcessorConfig;
    private final AwsCredentialsSupplier awsCredentialsSupplier;

    private static final String bedrockPayload = "{\"parameters\": {\"inputDataConfig\": {\"s3InputDataConfig\": {\"s3Uri\": \"s3://offlinebatch/my_batch2.jsonl\"}}," +
            "\"jobName\": \"batch-inference-from-connector\", \"outputDataConfig\": {\"s3OutputDataConfig\": {\"s3Uri\": \"s3://offlinebatch/bedrock-multisource/output-multisource/\"}}}}";

    public BedrockBatchJobCreator(final MLProcessorConfig mlProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier, final PluginMetrics pluginMetrics) {
        super(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics);
        this.mlProcessorConfig = mlProcessorConfig;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    @Override
    public void createMLBatchJob(Collection<Record<Event>> records) {
        records.stream().forEach(record -> {
            String bucket = record.getData().getJsonNode().get("bucket").asText();
            String key = record.getData().getJsonNode().get("key").asText();
            String s3Uri =  "s3://" + bucket + "/" + key;
            String payload = createPayloadBedrock(s3Uri, mlProcessorConfig);

            boolean success = retryWithBackoff(() -> sendRequestToMLCommons(payload, mlProcessorConfig, awsCredentialsSupplier));
            if (success) {
                incrementSuccessCounter();
            } else {
                incrementFailureCounter();
                LOG.error("Failed to create Bedrock batch job after multiple retries for: " + s3Uri);
            }
            // Add a 1ms delay to ensure next request is in a different millisecond
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.debug("Interrupted during sleep: " + e.getMessage());
            }
        });
    }

    private String createPayloadBedrock(String s3Url, MLProcessorConfig mlProcessorConfig) {
        String jobName = generateJobName();

        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(bedrockPayload);
            ((ObjectNode) rootNode.at("/parameters/inputDataConfig/s3InputDataConfig")).put("s3Uri", s3Url);
            ((ObjectNode) rootNode.at("/parameters")).put("jobName", jobName);
            ((ObjectNode) rootNode.at("/parameters/outputDataConfig/s3OutputDataConfig")).put("s3Uri", mlProcessorConfig.getOutputPath());

            return OBJECT_MAPPER.writeValueAsString(rootNode);
        } catch (IOException e) {
            throw new RuntimeException("Failed to construct JSON payload", e);
        }
    }
}
