/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class OpenSearchSinkConfigScriptValidationTest {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private OpenSearchSinkConfig deserialize(final String yaml) throws Exception {
        return YAML_MAPPER.readValue(yaml, OpenSearchSinkConfig.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"update", "upsert"})
    void script_with_update_or_upsert_action_is_valid(final String action) throws Exception {
        final String yaml = String.format(
                "action: \"%s\"\nscript:\n  source: \"ctx._source.putAll(params.doc)\"\n", action);
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isScriptActionValid(), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"index", "create", "delete"})
    void script_with_index_create_or_delete_action_is_invalid(final String action) throws Exception {
        final String yaml = String.format(
                "action: \"%s\"\nscript:\n  source: \"ctx._source.putAll(params.doc)\"\n", action);
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isScriptActionValid(), equalTo(false));
    }

    @Test
    void script_with_expression_action_is_valid() throws Exception {
        final String yaml = "action: \"${getMetadata(\\\"opensearch_action\\\")}\"\nscript:\n  source: \"ctx._source.putAll(params.doc)\"\n";
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isScriptActionValid(), equalTo(true));
    }

    @Test
    void no_script_is_always_valid() throws Exception {
        final String yaml = "action: \"index\"\n";
        final OpenSearchSinkConfig config = deserialize(yaml);
        assertThat(config.isScriptActionValid(), equalTo(true));
    }
}
