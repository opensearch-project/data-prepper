/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class SemanticEnrichmentConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testDeserialize_withFieldsAndMixedLanguages_success() throws Exception {
        final String field1 = UUID.randomUUID().toString();
        final String field2 = UUID.randomUUID().toString();
        final String json = String.format(
                "{\"fields\":[{\"%s\":\"english\"},{\"%s\":\"multilingual\"}]}", field1, field2);

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), notNullValue());
        assertThat(config.getFields().size(), equalTo(2));
        assertThat(config.getFields().get(0).get(field1), equalTo(SemanticEnrichmentLanguage.ENGLISH));
        assertThat(config.getFields().get(1).get(field2), equalTo(SemanticEnrichmentLanguage.MULTILINGUAL));
    }

    @Test
    void testDeserialize_withSingleField_success() throws Exception {
        final String field = UUID.randomUUID().toString();
        final String json = String.format("{\"fields\":[{\"%s\":\"english\"}]}", field);

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), notNullValue());
        assertThat(config.getFields().size(), equalTo(1));
        assertThat(config.getFields().get(0).get(field), equalTo(SemanticEnrichmentLanguage.ENGLISH));
    }

    @Test
    void testDeserialize_withEmptyFields_success() throws Exception {
        final String json = "{\"fields\":[]}";

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), notNullValue());
        assertThat(config.getFields().size(), equalTo(0));
    }

    @Test
    void testDeserialize_withNoFields_returnsNull() throws Exception {
        final String json = "{}";

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), nullValue());
    }

    @Test
    void testDeserialize_multipleFieldsInSingleMap_success() throws Exception {
        final String field1 = UUID.randomUUID().toString();
        final String field2 = UUID.randomUUID().toString();
        final String json = String.format(
                "{\"fields\":[{\"%s\":\"english\",\"%s\":\"multilingual\"}]}", field1, field2);

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), notNullValue());
        assertThat(config.getFields().size(), equalTo(1));
        final Map<String, SemanticEnrichmentLanguage> entry = config.getFields().get(0);
        assertThat(entry.get(field1), equalTo(SemanticEnrichmentLanguage.ENGLISH));
        assertThat(entry.get(field2), equalTo(SemanticEnrichmentLanguage.MULTILINGUAL));
    }

    @Test
    void testGetFields_returnsCorrectType() throws Exception {
        final String json = "{\"fields\":[{\"title\":\"english\"},{\"body\":\"multilingual\"}]}";

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        final List<Map<String, SemanticEnrichmentLanguage>> fields = config.getFields();
        assertThat(fields, notNullValue());
        assertThat(fields.size(), equalTo(2));
        assertThat(fields.get(0).get("title"), equalTo(SemanticEnrichmentLanguage.ENGLISH));
        assertThat(fields.get(1).get("body"), equalTo(SemanticEnrichmentLanguage.MULTILINGUAL));
    }
}
