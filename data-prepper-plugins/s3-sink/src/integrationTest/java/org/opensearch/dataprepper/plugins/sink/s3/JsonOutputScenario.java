/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class JsonOutputScenario implements OutputScenario {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public OutputCodec getCodec() {
        return new JsonOutputCodec(new JsonOutputCodecConfig());
    }

    @Override
    public void validate(int expectedRecords, List<Map<String, Object>> sampleEventData, File actualContentFile, CompressionScenario compressionScenario) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void validateDynamicPartition(int expectedRecords, int partitionNumber, File actualContentFile, CompressionScenario compressionScenario) throws IOException {
        JsonNode jsonArray = OBJECT_MAPPER.readTree(actualContentFile).get("events");

        int count = 0;
        for (final JsonNode eventNode : jsonArray) {
            final Integer sequence = eventNode.get("sequence").intValue();
            assertThat(sequence, equalTo(partitionNumber));
            count++;
        }

        if (expectedRecords != -1) {
            assertThat(count, equalTo(expectedRecords));
        }
    }

    @Override
    public String toString() {
        return "JSON";
    }
}
