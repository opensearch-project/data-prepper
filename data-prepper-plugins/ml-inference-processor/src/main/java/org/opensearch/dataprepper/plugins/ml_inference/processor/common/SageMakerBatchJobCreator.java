/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import org.json.JSONArray;
import org.json.JSONObject;
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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.HttpURLConnection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.LOG;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.client.S3ClientFactory.createS3Client;

public class SageMakerBatchJobCreator extends AbstractBatchJobCreator {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final S3Client s3Client;
    private final DateTimeFormatter dateTimeFormatter;
    private final Lock batchProcessingLock;
    // Added batch processing fields
    @Getter
    private final ConcurrentLinkedQueue<Record<Event>> batch_records = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Record<Event>> processedBatchRecords = new ConcurrentLinkedQueue<>();
    private final int maxBatchSize;
    private final AtomicLong lastUpdateTimestamp = new AtomicLong(-1);
    private static final long INACTIVITY_TIMEOUT_MS = 60000; // 1 minute in milliseconds

    private static final String SAGEMAKER_PAYLOAD_TEMPLATE = "{\"parameters\":{\"TransformInput\":{\"ContentType\":\"application/json\","
            + "\"DataSource\":{\"S3DataSource\":{\"S3DataType\":\"ManifestFile\",\"S3Uri\":\"\"}},"
            + "\"SplitType\":\"Line\"},\"TransformJobName\":\"\","
            + "\"TransformOutput\":{\"AssembleWith\":\"Line\",\"Accept\":\"application/json\","
            + "\"S3OutputPath\":\"s3://\"}}}";

    public SageMakerBatchJobCreator(final MLProcessorConfig mlProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier, final PluginMetrics pluginMetrics, final DlqPushHandler dlqPushHandler) {
        super(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics, dlqPushHandler);
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.s3Client = createS3Client(mlProcessorConfig, awsCredentialsSupplier);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        this.maxBatchSize = mlProcessorConfig.getMaxBatchSize();
        this.batchProcessingLock = new ReentrantLock();
    }

    @Override
    public void createMLBatchJob(List<Record<Event>> inputRecords, List<Record<Event>> resultRecords) {
        // Add new records to the batch and update timestamp
        if (inputRecords.isEmpty()) {
            return;
        }
        batch_records.addAll(inputRecords);
        lastUpdateTimestamp.set(System.currentTimeMillis());
        LOG.info("Added {} records to batch. Current batch size: {}",
                inputRecords.size(), batch_records.size());
    }

    /**
     * Adds any processed batch records to the provided result records list and clears the processed batch.
     *
     * @param resultRecords The list to add processed batch records to
     */
    @Override
    public void addProcessedBatchRecordsToResults(List<Record<Event>> resultRecords) {
        if (!batchProcessingLock.tryLock()) {
            LOG.debug("Another thread is currently processing results, skipping this attempt");
            return;
        }

        try {
            if (!processedBatchRecords.isEmpty()) {
                resultRecords.addAll(processedBatchRecords);
                LOG.info("Result records updated: {} processed records added, new total size: {}",
                        processedBatchRecords.size(), resultRecords.size());
                processedBatchRecords.clear();
            }
        } finally {
            batchProcessingLock.unlock();
        }
    }

    @Override
    public void checkAndProcessBatch() {
        if (!batchProcessingLock.tryLock()) {
            LOG.debug("Another thread is currently processing the current batch, skipping this attempt");
            return;
        }

        try {
            if (batch_records.isEmpty()) {
                return;
            }

            boolean shouldProcess = false;
            long currentTime = System.currentTimeMillis();
            long lastUpdate = lastUpdateTimestamp.get();

            // Check conditions for processing
            if (batch_records.size() >= maxBatchSize) {
                shouldProcess = true;
                LOG.info("Processing batch due to size limit reached: {}", batch_records.size());
            } else if (lastUpdate != -1 &&
                    currentTime - lastUpdate >= INACTIVITY_TIMEOUT_MS) {
                shouldProcess = true;
                LOG.info("Processing batch due to inactivity timeout. Time since last update: {} ms",
                        currentTime - lastUpdate);
            }

            if (shouldProcess) {
                List<Record<Event>> currentBatch = new ArrayList<>(batch_records);
                batch_records.clear();
                lastUpdateTimestamp.set(-1);

                processCurrentBatch(currentBatch);
            }
        } catch (Exception e) {
            LOG.error("Error in batch processing check: ", e);
        } finally {
            batchProcessingLock.unlock();
        }
    }

    private void processCurrentBatch(List<Record<Event>> currentBatch) {
        try {
            // Get the S3 manifest
            String customerBucket = currentBatch.stream()
                    .findAny()
                    .map(record -> {
                        return record.getData().getJsonNode().get("bucket").asText();
                    })
                    .orElse(null);   // Use null if no record is found
            String commonPrefix = findCommonPrefix(currentBatch);
            String manifestUrl = generateManifest(currentBatch, customerBucket, commonPrefix);
            String payload = createPayloadSageMaker(manifestUrl, mlProcessorConfig);

            RetryUtil.RetryResult result = RetryUtil.retryWithBackoffWithResult(
                    () -> mlCommonRequester.sendRequestToMLCommons(payload),
                    LOG
            );

            if (result.isSuccess()) {
                LOG.info("Successfully created SageMaker batch job for manifest URL: {}", manifestUrl);
                processedBatchRecords.addAll(currentBatch);
                incrementSuccessCounter();
                numberOfRecordsSuccessCounter.increment(currentBatch.size());
            } else {
                Exception lastException = result.getLastException();
                int statusCode;
                String errorMessage;

                if (lastException instanceof MLBatchJobException) {
                    MLBatchJobException mlException = (MLBatchJobException) lastException;
                    statusCode = mlException.getStatusCode();
                    errorMessage = String.format("Failed to Create SageMaker batch job after %d attempts: %s",
                            result.getAttemptsMade(), mlException.getMessage());
                } else {
                    statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
                    errorMessage = String.format("Failed to Create SageMaker batch job after %d attempts: %s",
                            result.getAttemptsMade(), lastException.getMessage());
                }

                handleFailure(currentBatch, processedBatchRecords, new MLBatchJobException(statusCode, errorMessage), statusCode);
                LOG.error("SageMaker batch job failed for manifest URL: {}. Status: {}, Error: {}", manifestUrl, statusCode, errorMessage);
            }
        } catch (IllegalArgumentException e) {
            LOG.error(NOISY, "Invalid arguments for SageMaker batch job. Error: {}", e.getMessage());
            handleFailure(currentBatch, processedBatchRecords, e, HttpURLConnection.HTTP_BAD_REQUEST);
        } catch (RuntimeException e) {
            LOG.error(NOISY, "Runtime Exception for SageMaker batch job. Error: {}", e.getMessage());
            handleFailure(currentBatch, processedBatchRecords, e, HttpURLConnection.HTTP_INTERNAL_ERROR);
        } catch (Exception e) {
            LOG.error(NOISY, "Unexpected Error occurred while creating a batch job through SageMaker: {}", e.getMessage(), e);
            handleFailure(currentBatch, processedBatchRecords, e, HttpURLConnection.HTTP_INTERNAL_ERROR);
        }
    }

    private void handleFailure(List<Record<Event>> failedRecords, ConcurrentLinkedQueue<Record<Event>> resultRecords, Throwable throwable, int statusCode) {
        if (failedRecords.isEmpty()) {
            incrementFailureCounter();
            return;
        }
        resultRecords.addAll(addFailureTags(failedRecords));
        incrementFailureCounter();
        numberOfRecordsFailedCounter.increment(failedRecords.size());
        if (dlqPushHandler == null) {
            return;
        }
        try {
            final List<DlqObject> dlqObjects = new ArrayList<>();
            for (Record<Event> record: failedRecords) {
                if (record.getData() != null) {
                    dlqObjects.add(createDlqObjectFromEvent(record.getData(), statusCode, throwable.getMessage()));
                }
            }
            dlqPushHandler.perform(dlqObjects);
        } catch (Exception ex) {
            LOG.error(NOISY, "Exception occured during error handling: {}", ex.getMessage());
        }
    }

    // Shutdown methods
    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return batch_records.isEmpty();
    }

    @Override
    public void shutdown() {
        processRemainingBatch();
        prepareForShutdown();
    }

    private void processRemainingBatch() {
        if (!batch_records.isEmpty()) {
            List<Record<Event>> currentBatch = new ArrayList<>(batch_records);
            batch_records.clear();
            processCurrentBatch(currentBatch);
        }
    }

    private String findCommonPrefix(Collection<Record<Event>> records) {
        EventKey inputKey = mlProcessorConfig.getInputKey();

        List<String> keys = records.stream()
                .map(record -> inputKey == null
                        ? record.getData().getJsonNode().get("key").asText()   // Use "key" from the JSON node if inputKey is null
                        : record.getData().get(inputKey, String.class)         // Use the inputKey if not null
                )
                .collect(Collectors.toList());

        if (keys.isEmpty()) throw new IllegalArgumentException("Empty inputs identified from input key : " + inputKey);

        // Handle single key case
        if (keys.size() == 1) {
            String singleKey = keys.get(0);
            int lastSlashIndex = singleKey.lastIndexOf('/');
            return lastSlashIndex >= 0 ? singleKey.substring(0, lastSlashIndex + 1) : "";
        }

        String prefix = keys.get(0);
        for (int i = 1; i < keys.size(); i++) {
            prefix = findCommonPrefix(prefix, keys.get(i));
            if (prefix.isEmpty()) break;
        }

        return prefix;
    }

    private String findCommonPrefix(String s1, String s2) {
        int minLength = Math.min(s1.length(), s2.length());
        int i = 0;

        while (i < minLength && s1.charAt(i) == s2.charAt(i)) {
            i++;
        }

        // Find the last occurrence of '/' before or at the mismatch
        int lastSlashIndex = s1.lastIndexOf('/', i - 1);
        return (lastSlashIndex >= 0) ? s1.substring(0, lastSlashIndex + 1) : "";
    }

    private String generateManifest(Collection<Record<Event>> records, String customerBucket, String prefix) {
        try {
            // Generate timestamp
            String timestamp = LocalDateTime.now().format(dateTimeFormatter);
            String folderName = prefix + "batch-" + timestamp;
            String fileName = folderName + "/batch-" + timestamp + ".manifest";

            // Construct JSON output
            JSONArray manifestArray = new JSONArray();
            manifestArray.put(new JSONObject().put("prefix", "s3://" + customerBucket + "/"));

            for (Record<Event> record : records) {
                String key = record.getData().getJsonNode().get("key").asText();
                manifestArray.put(key);
            }

            // Convert JSON to bytes
            byte[] jsonData = manifestArray.toString(4).getBytes();

            // Upload to S3
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(customerBucket)
                    .key(fileName)
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(jsonData));
            return "s3://" + customerBucket + "/" + fileName;
        } catch (Exception e) {
            LOG.error("Unexpected error while generating manifest file for SageMaker job.", e);
            return null;
        }
    }

    private String createPayloadSageMaker(String manifestUri, MLProcessorConfig mlProcessorConfig) {
        if (manifestUri == null || manifestUri.isEmpty()) {
            throw new IllegalArgumentException("Invalid manifest URI: manifestUri is either null or empty. Please ensure the correct input S3 uris are provided");
        }

        try {
            String jobName = generateJobName();
            String outputPath = mlProcessorConfig.getOutputPath();
            // If outputPath is not null, append jobName to it
            if (outputPath != null) {
                outputPath = outputPath.concat(outputPath.endsWith("/") ? "" : "/").concat(jobName);
            }

            JsonNode rootNode = OBJECT_MAPPER.readTree(SAGEMAKER_PAYLOAD_TEMPLATE);
            ((ObjectNode) rootNode.at("/parameters/TransformInput/DataSource/S3DataSource")).put("S3Uri", manifestUri);
            ((ObjectNode) rootNode.at("/parameters")).put("TransformJobName", jobName);

            // Only add TransformOutput if outputPath is not null
            if (outputPath != null) {
                ((ObjectNode) rootNode.at("/parameters/TransformOutput")).put("S3OutputPath", outputPath);
            } else {
                // Remove the TransformOutput section if outputPath is null
                ((ObjectNode) rootNode).remove("parameters").path("TransformOutput");
            }

            return OBJECT_MAPPER.writeValueAsString(rootNode);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to process the JSON payload for SageMaker batch job. Error: {}", e.getMessage());
            throw new RuntimeException("Error processing JSON payload for SageMaker batch job", e);
        } catch(Exception e) {
            LOG.error("Failed to create SageMaker batch job payload with input {}.", manifestUri, e);
            throw new RuntimeException("Failed to create payload for SageMaker batch job", e);
        }
    }

}
