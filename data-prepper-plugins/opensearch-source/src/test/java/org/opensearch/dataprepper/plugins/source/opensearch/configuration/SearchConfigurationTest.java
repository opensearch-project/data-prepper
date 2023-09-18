/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SearchConfigurationTest {


    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void default_search_configuration() {
        final SearchConfiguration searchConfiguration = new SearchConfiguration();

        assertThat(searchConfiguration.getBatchSize(), equalTo(1000));
    }

    @Test
    void non_default_search_configuration() {
        final Map<String, Object> pluginSettings = new HashMap<>();
        pluginSettings.put("batch_size", 2000);

        final SearchConfiguration searchConfiguration = objectMapper.convertValue(pluginSettings, SearchConfiguration.class);
        assertThat(searchConfiguration.getBatchSize(),equalTo(2000));
    }

    @Test
    void query_is_not_valid_json_string() {

        final Map<String, Object> pluginSettings = new HashMap<>();
        pluginSettings.put("batch_size", 1000);

        final SearchConfiguration searchConfiguration = objectMapper.convertValue(pluginSettings, SearchConfiguration.class);
        assertThat(searchConfiguration.getBatchSize(),equalTo(1000));
    }
}
