/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class SemanticEnrichmentConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testDeserialize_withFieldsAndLanguage_success() throws Exception {
        final String field1 = UUID.randomUUID().toString();
        final String field2 = UUID.randomUUID().toString();
        final String language = UUID.randomUUID().toString();
        final String json = String.format(
                "{\"fields\":[\"%s\",\"%s\"],\"language\":\"%s\"}", field1, field2, language);

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), notNullValue());
        assertThat(config.getFields().size(), equalTo(2));
        assertThat(config.getFields().get(0), equalTo(field1));
        assertThat(config.getFields().get(1), equalTo(field2));
        assertThat(config.getLanguage(), equalTo(language));
    }

    @Test
    void testDeserialize_withFieldsOnly_usesDefaultLanguage() throws Exception {
        final String field = UUID.randomUUID().toString();
        final String json = String.format("{\"fields\":[\"%s\"]}", field);

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), notNullValue());
        assertThat(config.getFields().size(), equalTo(1));
        assertThat(config.getFields().get(0), equalTo(field));
        assertThat(config.getLanguage(), equalTo(SemanticEnrichmentConfig.DEFAULT_LANGUAGE));
    }

    @Test
    void testDeserialize_withEmptyFields_success() throws Exception {
        final String json = "{\"fields\":[]}";

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), notNullValue());
        assertThat(config.getFields().size(), equalTo(0));
        assertThat(config.getLanguage(), equalTo(SemanticEnrichmentConfig.DEFAULT_LANGUAGE));
    }

    @Test
    void testDeserialize_withNoFields_returnsNull() throws Exception {
        final String json = "{}";

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), nullValue());
        assertThat(config.getLanguage(), equalTo(SemanticEnrichmentConfig.DEFAULT_LANGUAGE));
    }

    @Test
    void testDefaultLanguage_isEnglish() {
        assertThat(SemanticEnrichmentConfig.DEFAULT_LANGUAGE, equalTo("english"));
    }
}
