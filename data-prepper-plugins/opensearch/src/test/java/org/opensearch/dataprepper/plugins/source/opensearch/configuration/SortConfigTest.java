/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class SortConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    void default_order_is_ascending() throws Exception {
        final String yaml = "name: \"@timestamp\"\n";

        final SortConfig sortConfig = objectMapper.readValue(yaml, SortConfig.class);

        assertThat(sortConfig, notNullValue());
        assertThat(sortConfig.getName(), equalTo("@timestamp"));
        assertThat(sortConfig.getOrder(), equalTo("ascending"));
    }

    @Test
    void deserialization_with_descending_order() throws Exception {
        final String yaml = "name: \"@timestamp\"\norder: descending\n";

        final SortConfig sortConfig = objectMapper.readValue(yaml, SortConfig.class);

        assertThat(sortConfig, notNullValue());
        assertThat(sortConfig.getName(), equalTo("@timestamp"));
        assertThat(sortConfig.getOrder(), equalTo("descending"));
    }

    @Test
    void deserialization_with_ascending_order() throws Exception {
        final String yaml = "name: \"_id\"\norder: ascending\n";

        final SortConfig sortConfig = objectMapper.readValue(yaml, SortConfig.class);

        assertThat(sortConfig, notNullValue());
        assertThat(sortConfig.getName(), equalTo("_id"));
        assertThat(sortConfig.getOrder(), equalTo("ascending"));
    }

    @Test
    void valid_order_ascending_returns_true() throws Exception {
        final String yaml = "name: \"@timestamp\"\norder: ascending\n";

        final SortConfig sortConfig = objectMapper.readValue(yaml, SortConfig.class);

        assertThat(sortConfig.isOrderValid(), equalTo(true));
    }

    @Test
    void valid_order_descending_returns_true() throws Exception {
        final String yaml = "name: \"@timestamp\"\norder: descending\n";

        final SortConfig sortConfig = objectMapper.readValue(yaml, SortConfig.class);

        assertThat(sortConfig.isOrderValid(), equalTo(true));
    }

    @Test
    void invalid_order_returns_false() throws Exception {
        final String yaml = "name: \"@timestamp\"\norder: invalid\n";

        final SortConfig sortConfig = objectMapper.readValue(yaml, SortConfig.class);

        assertThat(sortConfig.isOrderValid(), equalTo(false));
    }
}
