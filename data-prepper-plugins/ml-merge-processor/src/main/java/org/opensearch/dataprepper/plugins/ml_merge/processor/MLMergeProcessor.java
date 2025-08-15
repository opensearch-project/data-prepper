/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_merge.processor;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Paths;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;


import static org.opensearch.dataprepper.plugins.ml_merge.common.client.S3ClientFactory.createS3Client;

@DataPrepperPlugin(name = "ml_merge", pluginType = Processor.class, pluginConfigurationType = MLMergeProcessorConfig.class)
public class MLMergeProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    public static final Logger LOG = LoggerFactory.getLogger(MLMergeProcessor.class);
    private final S3Client s3Client;
    private Map<String, Map<String, JsonNode>> cacheS3;
    private Map<String, String> outputFileToInputFile;

    private static String resultFileSuffix;
    private final String originalPrefix;


    @DataPrepperPluginConstructor
    public MLMergeProcessor(final MLMergeProcessorConfig mlMergeProcessorConfig, final PluginMetrics pluginMetrics, final AwsCredentialsSupplier awsCredentialsSupplier, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.s3Client = createS3Client(mlMergeProcessorConfig, awsCredentialsSupplier);
        cacheS3 = new HashMap();
        this.originalPrefix = mlMergeProcessorConfig.getSourceInput();
        this.resultFileSuffix = mlMergeProcessorConfig.getResultFileSuffix();
        this.outputFileToInputFile = new HashMap<>();
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        LOG.info("MLMergeProcessor received {} records", records.size());

        // read output file name
        for (Record<Event> record : records) {
            Event event = record.getData();
            String bucket = event.getJsonNode().get("s3").get("bucket").asText();
            String outputKey = event.getJsonNode().get("s3").get("key").asText();
            String outputFileUri = String.format("s3://%s/%s", bucket, outputKey);

            // extract the input file name
            String inputFileName = extractInputFileName(outputKey);

            // check the input file match the given prefix
            try {
                // Verify unique match in original prefix
                Map<String, JsonNode> inputMap = findAndLoadFile(bucket, inputFileName, outputFileUri);
                String recordId = event.getJsonNode().get("recordId").asText();
                JsonNode jsonNode = inputMap.get(recordId);

                jsonNode.fields().forEachRemaining(field -> {
                    String key = field.getKey();
                    JsonNode value = field.getValue();
                    event.put(key, value);
                    LOG.info("Key: {}, Value: {}", key, value.asText());
                });
            } catch (Exception e) {
                LOG.error("Error processing S3 files", e);
            }

        }


        // load the input file into a Map with recordId as the key

        return records;
    }

    private String findUniqueMatch(String bucket, String inputFileName, String outputFileUri) {
        try {
            if (outputFileToInputFile.containsKey(outputFileUri)) {
                return outputFileToInputFile.get(outputFileUri);
            }
            // List objects with the original prefix
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(originalPrefix)
                    .build();

            ListObjectsV2Response response = s3Client.listObjectsV2(listRequest);

            String matchedKey = null;
            int matchCount = 0;

            // Look for exact matches
            for (S3Object s3Object : response.contents()) {
                String key = s3Object.key();
                String fileName = Paths.get(key).getFileName().toString();

                if (fileName.equals(inputFileName)) {
                    matchedKey = key;
                    matchCount++;
                }
            }

            // Return the matched key only if exactly one match is found
            if (matchCount == 1) {
                outputFileToInputFile.put(outputFileUri, matchedKey);
                return matchedKey;
            } else if (matchCount == 0) {
                LOG.warn("No matching file found for: {}", inputFileName);
            } else {
                LOG.warn("Multiple matches ({}) found for: {}", matchCount, inputFileName);
            }

            return null;

        } catch (UnsupportedOperationException e) {
            LOG.error("Error accessing S3", e);
            throw e;
        } catch (Exception e) {
            LOG.error("Unexpected error occured ", e);
            throw e;
        }
    }

    private String extractInputFileName(String outputKey) {
        // Get the base filename
        String fileName = Paths.get(outputKey).getFileName().toString();
        if (fileName.endsWith(resultFileSuffix)) {
            return fileName.substring(0, fileName.length() - resultFileSuffix.length());
        }
        return fileName;
    }

    private Map<String, JsonNode> loadFileIntoMap(String bucket, String matchedKey) {
        try {
            String cacheKey = bucket + ":" + matchedKey;
            // Check if data is already in cache
            if (cacheS3.containsKey(cacheKey)) {
                LOG.info("Returning cached data for {}", cacheKey);
                return cacheS3.get(cacheKey);
            }

            // Create a map to store records
            Map<String, JsonNode> recordMap = new HashMap<>();

            // Get the object from S3
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(matchedKey)
                    .build();

            // Use ObjectMapper for JSON processing
            ObjectMapper objectMapper = new ObjectMapper();

            // Read the S3 object line by line
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(s3Client.getObject(getObjectRequest)))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    // Parse each line as JSON
                    JsonNode rootNode = objectMapper.readTree(line);

                    // Extract recordId
                    String recordId = rootNode.get("recordId").asText();

                    // Create a new object without the recordId
                    ObjectNode valueNode = objectMapper.createObjectNode();

                    // Iterate through all fields and copy everything except recordId
                    Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> field = fields.next();
                        String fieldName = field.getKey();
                        if (!fieldName.equals("recordId")) {
                            valueNode.set(fieldName, field.getValue());
                        }
                    }

                    // Add to map
                    recordMap.put(recordId, valueNode);
                }
            }

            LOG.info("Loaded {} records from file {}", recordMap.size(), matchedKey);
            cacheS3.put(cacheKey, recordMap);
            return recordMap;

        } catch (IOException e) {
            LOG.error("Error reading file from S3: {}", matchedKey, e);
            throw new RuntimeException("Error processing S3 file", e);
        }
    }

    // Modified findUniqueMatch to use the new method
    private Map<String, JsonNode> findAndLoadFile(String bucket, String inputFileName, String outputFileUri) {
        String matchedKey = findUniqueMatch(bucket, inputFileName, outputFileUri);
        if (matchedKey != null) {
            return loadFileIntoMap(bucket, matchedKey);
        }
        return Collections.emptyMap();
    }

    @Override
    public void prepareForShutdown() {}

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}