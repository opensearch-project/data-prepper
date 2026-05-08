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

class SemanticFieldMappingTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testDeserialize_withNameAndEnglishLanguage() throws Exception {
        final String name = UUID.randomUUID().toString();
        final String json = String.format("{\"name\":\"%s\",\"language\":\"english\"}", name);

        final SemanticFieldMapping mapping = OBJECT_MAPPER.readValue(json, SemanticFieldMapping.class);

        assertThat(mapping, notNullValue());
        assertThat(mapping.getName(), equalTo(name));
        assertThat(mapping.getLanguage(), equalTo(SemanticEnrichmentLanguage.ENGLISH));
    }

    @Test
    void testDeserialize_withNameAndMultilingualLanguage() throws Exception {
        final String name = UUID.randomUUID().toString();
        final String json = String.format("{\"name\":\"%s\",\"language\":\"multilingual\"}", name);

        final SemanticFieldMapping mapping = OBJECT_MAPPER.readValue(json, SemanticFieldMapping.class);

        assertThat(mapping, notNullValue());
        assertThat(mapping.getName(), equalTo(name));
        assertThat(mapping.getLanguage(), equalTo(SemanticEnrichmentLanguage.MULTILINGUAL));
    }

    @Test
    void testDeserialize_withOnlyName_languageIsNull() throws Exception {
        final String name = UUID.randomUUID().toString();
        final String json = String.format("{\"name\":\"%s\"}", name);

        final SemanticFieldMapping mapping = OBJECT_MAPPER.readValue(json, SemanticFieldMapping.class);

        assertThat(mapping.getName(), equalTo(name));
        assertThat(mapping.getLanguage(), nullValue());
    }

    @Test
    void testDeserialize_withOnlyLanguage_nameIsNull() throws Exception {
        final String json = "{\"language\":\"english\"}";

        final SemanticFieldMapping mapping = OBJECT_MAPPER.readValue(json, SemanticFieldMapping.class);

        assertThat(mapping.getName(), nullValue());
        assertThat(mapping.getLanguage(), equalTo(SemanticEnrichmentLanguage.ENGLISH));
    }

    @Test
    void testDeserialize_emptyJson_allFieldsNull() throws Exception {
        final String json = "{}";

        final SemanticFieldMapping mapping = OBJECT_MAPPER.readValue(json, SemanticFieldMapping.class);

        assertThat(mapping, notNullValue());
        assertThat(mapping.getName(), nullValue());
        assertThat(mapping.getLanguage(), nullValue());
    }

    @Test
    void testGetName_returnsCorrectValue() throws Exception {
        final String name = "my_text_field";
        final String json = String.format("{\"name\":\"%s\",\"language\":\"english\"}", name);

        final SemanticFieldMapping mapping = OBJECT_MAPPER.readValue(json, SemanticFieldMapping.class);

        assertThat(mapping.getName(), equalTo(name));
    }

    @Test
    void testGetLanguage_returnsCorrectEnumValue() throws Exception {
        final String json = "{\"name\":\"field1\",\"language\":\"multilingual\"}";

        final SemanticFieldMapping mapping = OBJECT_MAPPER.readValue(json, SemanticFieldMapping.class);

        assertThat(mapping.getLanguage(), equalTo(SemanticEnrichmentLanguage.MULTILINGUAL));
    }
}
