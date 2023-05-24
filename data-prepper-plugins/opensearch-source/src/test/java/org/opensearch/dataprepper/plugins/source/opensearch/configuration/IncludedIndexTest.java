/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class IncludedIndexTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void valid_index_name_regex() throws JsonProcessingException {
        final String includeIndexConfiguration = "index_name_regex: \"test_regex\"";
        final IncludedIndex includedIndex = objectMapper.readValue(includeIndexConfiguration, IncludedIndex.class);

        assertThat(includedIndex.isRegexValid(), equalTo(true));
        assertThat(includedIndex.getIndexNamePattern(), notNullValue());
        assertThat(includedIndex.getIndexNamePattern().pattern(), equalTo("test_regex"));
    }

    @Test
    void invalid_index_name_regex() throws JsonProcessingException {
        final String includeIndexConfiguration = "index_name_regex: \"[\"";
        final IncludedIndex includedIndex = objectMapper.readValue(includeIndexConfiguration, IncludedIndex.class);

        assertThat(includedIndex.isRegexValid(), equalTo(false));
    }
}
