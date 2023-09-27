/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3SelectJsonOptionTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void s3SelectJsonOptionWithLinesTest() throws JsonProcessingException {
        String jsonOptionYaml = "type: \"Lines\"";
        final S3SelectJsonOption jsonOption = objectMapper.readValue(jsonOptionYaml, S3SelectJsonOption.class);
        assertThat(jsonOption.getType(),equalTo("Lines"));
    }

    @Test
    void s3SelectJsonOptionWithDocumentTest() throws JsonProcessingException {
        String jsonOptionYaml = "type: \"DOCUMENT\"";
        final S3SelectJsonOption jsonOption = objectMapper.readValue(jsonOptionYaml, S3SelectJsonOption.class);
        assertThat(jsonOption.getType(),equalTo("DOCUMENT"));
    }

    @Test
    void s3SelectJsonOptionWithDefaultOptionTest() {
        S3SelectJsonOption jsonOption = new S3SelectJsonOption();
        assertThat(jsonOption.getType(),equalTo("DOCUMENT"));
    }
}
