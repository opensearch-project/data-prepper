/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.common.utils.RetryUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.opensearch.dataprepper.common.utils.RetryUtil.retryWithBackoffWithResult;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.LOG;

public class BedrockBatchJobCreator extends AbstractBatchJobCreator {
    private final AwsCredentialsSupplier awsCredentialsSupplier;

    private static final String BEDROCK_PAYLOAD_TEMPLATE = "{\"parameters\": {\"inputDataConfig\": {\"s3InputDataConfig\": {\"s3Uri\": \"s3://\"}}," +
            "\"jobName\": \"\", \"outputDataConfig\": {\"s3OutputDataConfig\": {\"s3Uri\": \"s3://\"}}}}";

    public BedrockBatchJobCreator(final MLProcessorConfig mlProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier, final PluginMetrics pluginMetrics, final DlqPushHandler dlqPushHandler) {
        super(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics, dlqPushHandler);
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    @Override
    public void createMLBatchJob(List<Record<Event>> inputRecords, List<Record<Event>> resultRecords) {
        List<Record<Event>> failedRecords = new ArrayList<>();
        List<DlqObject> dlqObjects = new ArrayList<>();  // Collect DLQ objects directly

        inputRecords.stream().forEach(record -> {
            try {
                String s3Uri = generateS3Uri(record);
                String payload = createPayloadBedrock(s3Uri, mlProcessorConfig);

                RetryUtil.RetryResult result = retryWithBackoffWithResult(
                        () -> mlCommonRequester.sendRequestToMLCommons(payload),
                        LOG
                );

                if (result.isSuccess()) {
                    LOG.info("Successfully created Bedrock batch job for the S3Uri: {}", s3Uri);
                    resultRecords.add(record);
                    incrementSuccessCounter();
                } else {
                    Exception e = result.getLastException();
                    String errorMessage = String.format(
                            "Failed to create Bedrock batch job after %d attempts for S3Uri: %s. Error: %s",
                            result.getAttemptsMade(),
                            s3Uri,
                            e.getMessage()
                    );

                    int statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
                    if (e instanceof MLBatchJobException) {
                        MLBatchJobException mlException = (MLBatchJobException) e;
                        statusCode = mlException.getStatusCode();
                        LOG.error(NOISY, errorMessage);
                    } else {
                        LOG.error(NOISY, errorMessage, e);
                    }

                    handleFailure(record, resultRecords, failedRecords, dlqObjects, e, statusCode);
                }
            } catch (IllegalArgumentException e) {
                LOG.error(NOISY, "Invalid arguments for BedRock batch job. Error: {}", e.getMessage());
                handleFailure(record, resultRecords, failedRecords, dlqObjects, e, HttpURLConnection.HTTP_BAD_REQUEST);
            } catch (Exception e) {
                LOG.error(NOISY, "Unexpected Error occurred while creating a batch job through BedRock. Error: {}",
                        e.getMessage(), e);
                handleFailure(record, resultRecords, failedRecords, dlqObjects, e, HttpURLConnection.HTTP_INTERNAL_ERROR);
            }
            // Add a 1ms delay to ensure next request is in a different millisecond
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.debug("Interrupted during sleep: " + e.getMessage());
            }
        });

        numberOfRecordsSuccessCounter.increment(inputRecords.size() - failedRecords.size());
        // If there are any failed records, throw the exception after all records are processed
        if (!failedRecords.isEmpty()) {
            pushToDlq(dlqObjects);
            numberOfRecordsFailedCounter.increment(dlqObjects.size());

            throw new MLBatchJobException(
                    String.format("Failed to process %d records out of %d total records",
                            failedRecords.size(),
                            inputRecords.size()),
                new Throwable("Batch job creation failed due to one or more failed records")
            );
        }
    }

    private void handleFailure(Record<Event> record,
                               List<Record<Event>> resultRecords,
                               List<Record<Event>> failedRecords,
                               List<DlqObject> dlqObjects,
                               Throwable throwable,
                               int statusCode) {
        resultRecords.addAll(addFailureTags(Collections.singletonList(record)));
        incrementFailureCounter();
        failedRecords.add(record); // Add to failed records

        if (dlqPushHandler == null) {
            return;
        }
        try {
            if (record.getData() != null) {
                dlqObjects.add(createDlqObjectFromEvent(
                        record.getData(),
                        statusCode,
                        throwable.getMessage()
                ));
            }
        } catch (Exception ex) {
            LOG.error(NOISY, "Exception occured during error handling: {}", ex.getMessage());
        }
    }

    private void pushToDlq(List<DlqObject> dlqObjects) {
        if (dlqPushHandler == null || dlqObjects.isEmpty()) {
            return;
        }

        try {
            dlqPushHandler.perform(dlqObjects);
            LOG.info("Successfully pushed {} failed records to DLQ", dlqObjects.size());
        } catch (Exception e) {
            LOG.error("Failed to push {} records to DLQ: {}", dlqObjects.size(), e.getMessage(), e);
        }
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
