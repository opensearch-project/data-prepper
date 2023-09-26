/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.ExportSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class ManifestFileReader {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileReader.class);

    private static final String DATA_FILE_S3_KEY = "dataFileS3Key";
    private static final String DATA_FILE_ITEM_COUNT_KEY = "itemCount";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final S3ObjectReader fileReader;

    public ManifestFileReader(S3ObjectReader fileReader) {
        this.fileReader = fileReader;
    }

    public ExportSummary parseSummaryFile(String bucket, String key) {
        LOG.debug("Try to read the manifest summary file");
        InputStream object = fileReader.readFile(bucket, key);

        BufferedReader reader = new BufferedReader(new InputStreamReader(object));
        try {
            String line = reader.readLine();
            LOG.debug("Manifest summary: " + line);
            ExportSummary summaryInfo = MAPPER.readValue(line, ExportSummary.class);
            return summaryInfo;

        } catch (JsonProcessingException e) {
            LOG.error("Failed to parse the summary info due to :" + e.getMessage());
            throw new RuntimeException(e);

        } catch (IOException e) {
            LOG.error("IO Exception due to :" + e.getMessage());
            throw new RuntimeException(e);
        }

    }

    public Map<String, Integer> parseDataFile(String bucket, String key) {
        LOG.debug("Try to read the manifest data file");

        Map<String, Integer> result = new HashMap<>();
        InputStream object = fileReader.readFile(bucket, key);
        BufferedReader reader = new BufferedReader(new InputStreamReader(object));

        String line;
        try {
            while ((line = reader.readLine()) != null) {
                // An example line as below:
                // {"itemCount":46331,"md5Checksum":"a0k21IY3eelgr2PuWJLjJw==","etag":"51f9f394903c5d682321c6211aae8b6a-1","dataFileS3Key":"test-table-export/AWSDynamoDB/01692350182719-6de2c037/data/fpgzwz7ome3s7a5gqn2mu3ogtq.json.gz"}
                Map<String, String> map = MAPPER.readValue(line, new TypeReference<>() {
                });
                LOG.debug("Get a file {} with item count {}", map.get(DATA_FILE_S3_KEY), map.get(DATA_FILE_ITEM_COUNT_KEY));
                result.put(map.get(DATA_FILE_S3_KEY), Integer.valueOf(map.get(DATA_FILE_ITEM_COUNT_KEY)));

            }
        } catch (IOException e) {
            LOG.error("IO Exception due to " + e.getMessage());
        }

        return result;
    }
}
