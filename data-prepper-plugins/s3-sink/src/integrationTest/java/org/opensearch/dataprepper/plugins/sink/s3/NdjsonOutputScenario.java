/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonOutputConfig;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class NdjsonOutputScenario implements OutputScenario {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public OutputCodec getCodec() {
        return new NdjsonOutputCodec(new NdjsonOutputConfig());
    }

    @Override
    public void validate(int expectedRecords, final List<Map<String, Object>> sampleEventData, final File actualContentFile, CompressionScenario compressionScenario) throws IOException {
        final InputStream inputStream = new BufferedInputStream(new FileInputStream(actualContentFile), 64 * 1024);

        final Scanner scanner = new Scanner(inputStream);

        int count = 0;
        int sampledData = 0;
        while (scanner.hasNext()) {

            final String actualJsonString = scanner.next();

            final Map<String, Object> actualData = OBJECT_MAPPER.readValue(actualJsonString, Map.class);

            if(count < sampleEventData.size()) {
                final Map<String, Object> expectedData = sampleEventData.get(count);
                assertThat(actualData, equalTo(expectedData));
                sampledData++;
            }
            count++;
        }

        assertThat(count, equalTo(expectedRecords));
        assertThat(sampledData, equalTo(sampleEventData.size()));
    }

    public void validateDynamicPartition(int expectedRecords, int partitionNumber, final File actualContentFile, final CompressionScenario compressionScenario) throws IOException {
        final InputStream inputStream = new BufferedInputStream(new FileInputStream(actualContentFile), 64 * 1024);

        final Scanner scanner = new Scanner(inputStream);

        int count = 0;
        while (scanner.hasNext()) {

            final String actualJsonString = scanner.next();

            final Map<String, Object> actualData = OBJECT_MAPPER.readValue(actualJsonString, Map.class);
            assertThat(actualData.get("sequence"), equalTo(partitionNumber));
            count++;
        }

        if (expectedRecords != -1) {
            assertThat(count, equalTo(expectedRecords));
        }
    }

    @Override
    public String toString() {
        return "NDJSON";
    }
}
