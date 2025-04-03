/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.LOG;
import static org.opensearch.dataprepper.common.utils.RetryUtil.retryWithBackoff;

public class BedrockBatchJobCreator extends AbstractBatchJobCreator {
    private final AwsCredentialsSupplier awsCredentialsSupplier;

    private static final String BEDROCK_PAYLOAD_TEMPLATE = "{\"parameters\": {\"inputDataConfig\": {\"s3InputDataConfig\": {\"s3Uri\": \"s3://\"}}," +
            "\"jobName\": \"\", \"outputDataConfig\": {\"s3OutputDataConfig\": {\"s3Uri\": \"s3://\"}}}}";

    public BedrockBatchJobCreator(final MLProcessorConfig mlProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier, final PluginMetrics pluginMetrics) {
        super(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics);
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    @Override
    public void createMLBatchJob(List<Record<Event>> inputRecords, List<Record<Event>> resultRecords) {
        List<Record<Event>> failedRecords = new ArrayList<>();
        inputRecords.stream().forEach(record -> {
            try {
                String s3Uri = generateS3Uri(record);
                String payload = createPayloadBedrock(s3Uri, mlProcessorConfig);

                boolean success = retryWithBackoff(() -> mlCommonRequester.sendRequestToMLCommons(payload), LOG);
                if (success) {
                    LOG.info("Successfully created Bedrock batch job for the S3Uri: {}", s3Uri);
                    resultRecords.add(record);
                    incrementSuccessCounter();
                } else {
                    handleFailure(record, resultRecords, failedRecords);
                    LOG.error("Failed to create Bedrock batch job after multiple retries for: " + s3Uri);
                }
            } catch (IllegalArgumentException e) {
                LOG.error(NOISY, "Invalid arguments for BedRock batch job. Error: {}", e.getMessage());
                handleFailure(record, resultRecords, failedRecords);
            } catch (Exception e) {
                LOG.error(NOISY, "Unexpected Error occurred while creating a batch job through BedRock. Error: {}", e.getMessage(), e);
                handleFailure(record, resultRecords, failedRecords);
            }
            // Add a 1ms delay to ensure next request is in a different millisecond
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.debug("Interrupted during sleep: " + e.getMessage());
            }
        });

        // If there are any failed records, throw the exception after all records are processed
        if (!failedRecords.isEmpty()) {
            throw new MLBatchJobException(
                "Failed to process the following records: " + failedRecords,
                new Throwable("Batch job creation failed due to one or more failed records")
            );
        }
    }

    private void handleFailure(Record<Event> record, List<Record<Event>> resultRecords, List<Record<Event>> failedRecords) {
        resultRecords.addAll(addFailureTags(Collections.singletonList(record)));
        incrementFailureCounter();
        failedRecords.add(record); // Add to failed records
    }

    private String generateS3Uri(Record<Event> record) {
        String bucket = Optional.ofNullable(record.getData().getJsonNode().path("bucket").asText(null))
                .orElseThrow(() -> new IllegalArgumentException("Missing 'bucket' in record."));

        EventKey inputKey = mlProcessorConfig.getInputKey();
        String key = Optional.ofNullable(inputKey == null
                        ? record.getData().getJsonNode().path("key").asText(null)
                        : record.getData().get(inputKey, String.class))
                .orElseThrow(() -> new IllegalArgumentException("Missing 'S3 Key' in record."));
        return "s3://" + bucket + "/" + key;
    }

    private String createPayloadBedrock(String S3Uri, MLProcessorConfig mlProcessorConfig) {
        if (S3Uri == null || S3Uri.isEmpty()) {
            throw new IllegalArgumentException("Invalid S3Uri: S3Uri is either null or empty. Please ensure the correct input S3 uris are provided");
        }
        String jobName = generateJobName();

        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(BEDROCK_PAYLOAD_TEMPLATE);
            ((ObjectNode) rootNode.at("/parameters/inputDataConfig/s3InputDataConfig")).put("s3Uri", S3Uri);
            ((ObjectNode) rootNode.at("/parameters")).put("jobName", jobName);
            ((ObjectNode) rootNode.at("/parameters/outputDataConfig/s3OutputDataConfig")).put("s3Uri", mlProcessorConfig.getOutputPath());

            return OBJECT_MAPPER.writeValueAsString(rootNode);
        } catch (Exception e) {
            LOG.error("Failed to create BedRock batch job payload with input {}.", S3Uri, e);
            throw new RuntimeException("Failed to create payload for BedRock batch job", e);
        }
    }
}
