/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class IcebergSourceConfigTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void default_catalog_is_empty() throws Exception {
        final IcebergSourceConfig config = MAPPER.readValue(
                "{\"tables\": [{\"table_name\": \"db.t\"}]}", IcebergSourceConfig.class);
        assertThat(config.getCatalog().isEmpty(), is(true));
    }

    @Test
    void top_level_catalog_is_deserialized() throws Exception {
        final String json = "{\"catalog\": {\"type\": \"rest\", \"uri\": \"http://localhost:8181\"}, " +
                "\"tables\": [{\"table_name\": \"db.t\"}]}";
        final IcebergSourceConfig config = MAPPER.readValue(json, IcebergSourceConfig.class);
        assertThat(config.getCatalog(), equalTo(Map.of("type", "rest", "uri", "http://localhost:8181")));
    }
}
