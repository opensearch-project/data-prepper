/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class S3SelectOptionsTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void s3_select_options_with_default_csv_json_test() throws JsonProcessingException {
        final String s3SelectYamlConfiguration ="        expression: \"SELECT * FROM s3object s\"\n" +
                "        input_serialization: csv";
        final S3SelectOptions s3SelectOptions = objectMapper.readValue(s3SelectYamlConfiguration, S3SelectOptions.class);
        assertThat(s3SelectOptions.getS3SelectSerializationFormatOption(),sameInstance(S3SelectSerializationFormatOption.CSV));
        assertThat(s3SelectOptions.getExpression(),equalTo("SELECT * FROM s3object s"));
        assertThat(s3SelectOptions.getExpressionType(),equalTo("SQL"));
        assertThat(s3SelectOptions.getCompressionType(),equalTo("none"));
        assertThat(s3SelectOptions.getS3SelectCSVOption(),instanceOf(S3SelectCSVOption.class));
        assertThat(s3SelectOptions.getS3SelectCSVOption().getFileHeaderInfo(),equalTo("USE"));
        assertThat(s3SelectOptions.getS3SelectCSVOption().getComments(),nullValue());
        assertThat(s3SelectOptions.getS3SelectCSVOption().getQuiteEscape(),nullValue());
        assertThat(s3SelectOptions.getS3SelectJsonOption(),instanceOf(S3SelectJsonOption.class));
        assertThat(s3SelectOptions.getS3SelectJsonOption().getType(),equalTo("DOCUMENT"));
    }
}
