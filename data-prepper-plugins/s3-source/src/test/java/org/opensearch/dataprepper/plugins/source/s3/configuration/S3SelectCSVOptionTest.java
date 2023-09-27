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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3SelectCSVOptionTest {

    @Test
    void s3SelectOptions_has_correct_defaults() {
        S3SelectCSVOption csvOption = new S3SelectCSVOption();
        assertThat(csvOption.getFileHeaderInfo(),equalTo("USE"));
        assertThat(csvOption.getComments(),nullValue());
        assertThat(csvOption.getQuiteEscape(),nullValue());
    }

    @Test
    void s3SelectOptions_has_correct_values() throws JsonProcessingException {
        ObjectMapper objectMapper =
                new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));
        String yaml = "file_header_info: \"none\" \nquote_escape: \".\" \ncomments: \"$\"";
        final S3SelectCSVOption s3SelectCSVOption = objectMapper.readValue(yaml, S3SelectCSVOption.class);
        assertThat(s3SelectCSVOption.getFileHeaderInfo(),equalTo("none"));
        assertThat(s3SelectCSVOption.getComments(),equalTo("$"));
        assertThat(s3SelectCSVOption.getQuiteEscape(),equalTo("."));
    }
}
