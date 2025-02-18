/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml.processor.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.ml.processor.MLProcessorConfig;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.opensearch.dataprepper.plugins.ml.processor.MLProcessor.LOG;
import static org.opensearch.dataprepper.plugins.ml.processor.client.S3ClientFactory.createS3Client;
import static org.opensearch.dataprepper.plugins.ml.processor.util.MlCommonRequester.sendRequestToMLCommons;
import static org.opensearch.dataprepper.plugins.ml.processor.util.RetryUtil.retryWithBackoff;

public class SageMakerBatchJobCreator extends AbstractBatchJobCreator {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final MLProcessorConfig mlProcessorConfig;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final S3Client s3Client;

    private static final String sagemakerPayload = "{\"parameters\":{\"TransformInput\":{\"ContentType\":\"application/json\","
            + "\"DataSource\":{\"S3DataSource\":{\"S3DataType\":\"ManifestFile\",\"S3Uri\":\"\"}},"
            + "\"SplitType\":\"Line\"},\"TransformJobName\":\"\","
            + "\"TransformOutput\":{\"AssembleWith\":\"Line\",\"Accept\":\"application/json\","
            + "\"S3OutputPath\":\"s3://offlinebatch/sagemaker/output\"}}}";

    public SageMakerBatchJobCreator(final MLProcessorConfig mlProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier, final PluginMetrics pluginMetrics) {
        super(mlProcessorConfig, awsCredentialsSupplier, pluginMetrics);
        this.mlProcessorConfig = mlProcessorConfig;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.s3Client = createS3Client(mlProcessorConfig, awsCredentialsSupplier);
    }

    @Override
    public void createMLBatchJob(Collection<Record<Event>> records) {
        // Get the S3 manifest
        String customerBucket = records.stream()
                .findAny()
                .map(record -> {
                    return record.getData().getJsonNode().get("bucket").asText();
                })
                .orElse(null);   // Use null if no record is found
        String commonPrefix = findCommonPrefix(records);
        String manifestUrl = generateManifest(records, customerBucket, commonPrefix, mlProcessorConfig);
        String payload = createPayloadSageMaker(manifestUrl, mlProcessorConfig);

        boolean success = retryWithBackoff(() -> sendRequestToMLCommons(payload, mlProcessorConfig, awsCredentialsSupplier));
        if (success) {
            incrementSuccessCounter();
        } else {
            incrementFailureCounter();
            LOG.error("Failed to create SageMaker batch job after multiple retries for: " + manifestUrl);
        }
    }

    private String findCommonPrefix(Collection<Record<Event>> records) {
        List<String> keys = new ArrayList<>();
        for (Record<Event> record : records) {
            keys.add(record.getData().getJsonNode().get("key").asText());
        }

        if (keys.isEmpty()) return "";

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


    private String generateManifest(Collection<Record<Event>> records, String customerBucket, String prefix, MLProcessorConfig mlProcessorConfig) {
        try {
            // Generate timestamp
            String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
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
            LOG.error("Unexpected error while create SageMaker job.", e);
            return null;
        }
    }

    private String createPayloadSageMaker(String s3Url, MLProcessorConfig mlProcessorConfig) {
        String jobName = generateJobName();

        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(sagemakerPayload);
            ((ObjectNode) rootNode.at("/parameters/TransformInput/DataSource/S3DataSource")).put("S3Uri", s3Url);
            ((ObjectNode) rootNode.at("/parameters")).put("TransformJobName", jobName);
            ((ObjectNode) rootNode.at("/parameters/TransformOutput")).put("S3OutputPath", mlProcessorConfig.getOutputPath());

            return OBJECT_MAPPER.writeValueAsString(rootNode);
        } catch (IOException e) {
            throw new RuntimeException("Failed to construct JSON payload", e);
        }
    }

}
