/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class SemanticEnrichmentConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testDeserialize_withMultipleFields_success() throws Exception {
        final String field1 = UUID.randomUUID().toString();
        final String field2 = UUID.randomUUID().toString();
        final String json = String.format(
                "{\"fields\":[{\"name\":\"%s\",\"language\":\"english\"},{\"name\":\"%s\",\"language\":\"multilingual\"}]}",
                field1, field2);

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), notNullValue());
        assertThat(config.getFields().size(), equalTo(2));
        assertThat(config.getFields().get(0).getName(), equalTo(field1));
        assertThat(config.getFields().get(0).getLanguage(), equalTo(SemanticEnrichmentLanguage.ENGLISH));
        assertThat(config.getFields().get(1).getName(), equalTo(field2));
        assertThat(config.getFields().get(1).getLanguage(), equalTo(SemanticEnrichmentLanguage.MULTILINGUAL));
    }

    @Test
    void testDeserialize_withSingleField_success() throws Exception {
        final String field = UUID.randomUUID().toString();
        final String json = String.format("{\"fields\":[{\"name\":\"%s\",\"language\":\"english\"}]}", field);

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), notNullValue());
        assertThat(config.getFields().size(), equalTo(1));
        assertThat(config.getFields().get(0).getName(), equalTo(field));
        assertThat(config.getFields().get(0).getLanguage(), equalTo(SemanticEnrichmentLanguage.ENGLISH));
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
    void testDeserialize_withMixedLanguages_success() throws Exception {
        final String json = "{\"fields\":["
                + "{\"name\":\"title\",\"language\":\"english\"},"
                + "{\"name\":\"body\",\"language\":\"multilingual\"},"
                + "{\"name\":\"summary\",\"language\":\"english\"}"
                + "]}";

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        final List<SemanticFieldMapping> fields = config.getFields();
        assertThat(fields, notNullValue());
        assertThat(fields.size(), equalTo(3));
        assertThat(fields.get(0).getName(), equalTo("title"));
        assertThat(fields.get(0).getLanguage(), equalTo(SemanticEnrichmentLanguage.ENGLISH));
        assertThat(fields.get(1).getName(), equalTo("body"));
        assertThat(fields.get(1).getLanguage(), equalTo(SemanticEnrichmentLanguage.MULTILINGUAL));
        assertThat(fields.get(2).getName(), equalTo("summary"));
        assertThat(fields.get(2).getLanguage(), equalTo(SemanticEnrichmentLanguage.ENGLISH));
    }

    @Test
    void testDeserialize_fieldWithoutLanguage_languageIsNull() throws Exception {
        final String name = UUID.randomUUID().toString();
        final String json = String.format("{\"fields\":[{\"name\":\"%s\"}]}", name);

        final SemanticEnrichmentConfig config = OBJECT_MAPPER.readValue(json, SemanticEnrichmentConfig.class);

        assertThat(config.getFields(), notNullValue());
        assertThat(config.getFields().size(), equalTo(1));
        assertThat(config.getFields().get(0).getName(), equalTo(name));
        assertThat(config.getFields().get(0).getLanguage(), nullValue());
    }
}
