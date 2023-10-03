/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class SearchConfigurationTest {


    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void default_search_configuration() {
        final SearchConfiguration searchConfiguration = new SearchConfiguration();

        assertThat(searchConfiguration.getBatchSize(), equalTo(1000));
        assertThat(searchConfiguration.getSearchContextType(), nullValue());
    }

    @Test
    void non_default_search_configuration() {
        final Map<String, Object> pluginSettings = new HashMap<>();
        pluginSettings.put("batch_size", 2000);
        pluginSettings.put("search_context_type", "scroll");

        final SearchConfiguration searchConfiguration = objectMapper.convertValue(pluginSettings, SearchConfiguration.class);
        assertThat(searchConfiguration.getBatchSize(),equalTo(2000));
        assertThat(searchConfiguration.isSearchContextTypeValid(), equalTo(true));
        assertThat(searchConfiguration.getSearchContextType(), equalTo(SearchContextType.SCROLL));
    }

    @Test
    void search_context_type_invalid() {

        final Map<String, Object> pluginSettings = new HashMap<>();
        pluginSettings.put("batch_size", 1000);
        pluginSettings.put("search_context_type", "invalid");


        final SearchConfiguration searchConfiguration = objectMapper.convertValue(pluginSettings, SearchConfiguration.class);
        assertThat(searchConfiguration.getBatchSize(),equalTo(1000));

        assertThat(searchConfiguration.isSearchContextTypeValid(), equalTo(false));
        assertThat(searchConfiguration.getSearchContextType(), nullValue());
    }
}
