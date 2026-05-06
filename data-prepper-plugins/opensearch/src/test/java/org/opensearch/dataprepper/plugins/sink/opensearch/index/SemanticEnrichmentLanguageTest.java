/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SemanticEnrichmentLanguageTest {

    @Test
    void test_english_hasCorrectValue() {
        assertThat(SemanticEnrichmentLanguage.ENGLISH.getValue(), equalTo("english"));
    }

    @Test
    void test_multilingual_hasCorrectValue() {
        assertThat(SemanticEnrichmentLanguage.MULTILINGUAL.getValue(), equalTo("multilingual"));
    }

    @Test
    void test_fromValue_english_returnsEnglish() {
        assertThat(SemanticEnrichmentLanguage.fromValue("english"), equalTo(SemanticEnrichmentLanguage.ENGLISH));
    }

    @Test
    void test_fromValue_multilingual_returnsMultilingual() {
        assertThat(SemanticEnrichmentLanguage.fromValue("multilingual"), equalTo(SemanticEnrichmentLanguage.MULTILINGUAL));
    }

    @Test
    void test_fromValue_caseInsensitive_english() {
        assertThat(SemanticEnrichmentLanguage.fromValue("ENGLISH"), equalTo(SemanticEnrichmentLanguage.ENGLISH));
    }

    @Test
    void test_fromValue_caseInsensitive_multilingual() {
        assertThat(SemanticEnrichmentLanguage.fromValue("Multilingual"), equalTo(SemanticEnrichmentLanguage.MULTILINGUAL));
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "spanish", "", "eng"})
    void test_fromValue_invalidValue_throwsException(final String value) {
        assertThrows(IllegalArgumentException.class, () -> SemanticEnrichmentLanguage.fromValue(value));
    }
}
