/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class OpenSearchSinkConfig_PullIndexingValidationTest {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private static final String PULL_INDEXING_BLOCK = "pull_indexing:\n  engine:\n    kafka:\n      bootstrap_servers:\n        - \"localhost:9092\"\n";

    private OpenSearchSinkConfig deserialize(final String yaml) throws Exception {
        return YAML_MAPPER.readValue(yaml, OpenSearchSinkConfig.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"update", "upsert"})
    void isPullIndexingActionValid_returns_false_for_unsupported_action(final String action) throws Exception {
        final String yaml = String.format("action: \"%s\"\n%s", action, PULL_INDEXING_BLOCK);
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isPullIndexingActionValid(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"index", "create", "delete"})
    void isPullIndexingActionValid_returns_true_for_supported_action(final String action) throws Exception {
        final String yaml = String.format("action: \"%s\"\n%s", action, PULL_INDEXING_BLOCK);
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isPullIndexingActionValid(), equalTo(true));
    }

    @Test
    void isPullIndexingActionValid_returns_true_for_expression_action() throws Exception {
        final String yaml = String.format("action: \"${getMetadata(\\\"opensearch_action\\\")}\"\n%s", PULL_INDEXING_BLOCK);
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isPullIndexingActionValid(), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"update", "upsert"})
    void isPullIndexingActionValid_returns_false_for_unsupported_action_in_actions_list(final String action) throws Exception {
        final String yaml = String.format("actions:\n  - type: \"%s\"\n    when: \"/some_field == true\"\n%s", action, PULL_INDEXING_BLOCK);
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isPullIndexingActionValid(), equalTo(false));
    }

    @Test
    void isPullIndexingActionValid_returns_true_when_pull_indexing_is_not_configured() throws Exception {
        final String yaml = "action: \"update\"\n";
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isPullIndexingActionValid(), equalTo(true));
    }

    @Test
    void isPullIndexingIndexValid_returns_false_for_dynamic_index() throws Exception {
        final String yaml = String.format("index: \"my-index-${/index_field}\"\n%s", PULL_INDEXING_BLOCK);
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isPullIndexingIndexValid(), equalTo(false));
    }

    @Test
    void isPullIndexingIndexValid_returns_true_for_static_index() throws Exception {
        final String yaml = String.format("index: \"my-index\"\n%s", PULL_INDEXING_BLOCK);
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isPullIndexingIndexValid(), equalTo(true));
    }

    @Test
    void isPullIndexingIndexValid_returns_true_when_pull_indexing_is_not_configured() throws Exception {
        final String yaml = "index: \"my-index-${/index_field}\"\n";
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isPullIndexingIndexValid(), equalTo(true));
    }
}
