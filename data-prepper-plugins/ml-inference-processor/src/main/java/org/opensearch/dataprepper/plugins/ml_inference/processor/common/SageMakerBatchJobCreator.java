/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.common.utils.RetryUtil.retryWithBackoff;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessor.LOG;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.client.S3ClientFactory.createS3Client;

public class SageMakerBatchJobCreator extends AbstractBatchJobCreator {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final S3Client s3Client;
    private final DateTimeFormatter dateTimeFormatter;

    private static final String SAGEMAKER_PAYLOAD_TEMPLATE = "{\"parameters\":{\"TransformInput\":{\"ContentType\":\"application/json\","
            + "\"DataSource\":{\"S3DataSource\":{\"S3DataType\":\"ManifestFile\",\"S3Uri\":\"\"}},"
            + "\"SplitType\":\"Line\"},\"TransformJobName\":\"\","
            + "\"TransformOutput\":{\"AssembleWith\":\"Line\",\"Accept\":\"application/json\","
            + "\"S3OutputPath\":\"s3://\"}}}";

    public SageMakerBatchJobCreator(final MLProcessorConfig mlProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier, final PluginMetrics pluginMetrics) {
        super(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics);
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.s3Client = createS3Client(mlProcessorConfig, awsCredentialsSupplier);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    }

    @Override
    public void createMLBatchJob(List<Record<Event>> inputRecords, List<Record<Event>> resultRecords) {
        try {
            // Get the S3 manifest
            String customerBucket = inputRecords.stream()
                    .findAny()
                    .map(record -> {
                        return record.getData().getJsonNode().get("bucket").asText();
                    })
                    .orElse(null);   // Use null if no record is found
            String commonPrefix = findCommonPrefix(inputRecords);
            String manifestUrl = generateManifest(inputRecords, customerBucket, commonPrefix);
            String payload = createPayloadSageMaker(manifestUrl, mlProcessorConfig);

            boolean success = retryWithBackoff(() -> mlCommonRequester.sendRequestToMLCommons(payload), LOG);
            if (success) {
                LOG.info("Successfully created SageMaker batch job for manifest URL: {}", manifestUrl);
                resultRecords.addAll(inputRecords);
                incrementSuccessCounter();
            } else {
                handleFailure(inputRecords, resultRecords);
                LOG.error("SageMaker batch job failed after multiple retries for manifest URL: {}", manifestUrl);
                throw new MLBatchJobException(
                    "Failed to create SageMaker batch job in ml-commons for manifest URL: " + manifestUrl,
                    new Throwable("Batch job creation failed after multiple retries")
                );
            }
        } catch (IllegalArgumentException e) {
            LOG.error(NOISY, "Invalid arguments for SageMaker batch job. Error: {}", e.getMessage());
            handleFailure(inputRecords, resultRecords);
            throw new MLBatchJobException("Failed to create SageMaker batch job due to invalid arguments.", e);
        } catch (RuntimeException e) {
            LOG.error(NOISY, "Runtime Exception for SageMaker batch job. Error: {}", e.getMessage());
            handleFailure(inputRecords, resultRecords);
            throw new MLBatchJobException("Failed to create SageMaker batch job due to Runtime Exception.", e);
        } catch (Exception e) {
            LOG.error(NOISY, "Unexpected Error occurred while creating a batch job through SageMaker: {}", e.getMessage(), e);
            handleFailure(inputRecords, resultRecords);
            throw new MLBatchJobException("Failed to create SageMaker batch job due to unexpected error.", e);
        }
    }

    private void handleFailure(List<Record<Event>> record, List<Record<Event>> resultRecords) {
        resultRecords.addAll(addFailureTags(record));
        incrementFailureCounter();
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
