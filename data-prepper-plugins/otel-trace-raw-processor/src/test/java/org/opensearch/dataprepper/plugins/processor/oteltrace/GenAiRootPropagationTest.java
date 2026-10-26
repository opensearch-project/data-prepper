/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GenAiRootPropagationTest {

    @ParameterizedTest
    @EnumSource(GenAiRootPropagation.class)
    void fromOptionValue_readsTheOptionValueOfEachMode(final GenAiRootPropagation mode) {
        assertThat(GenAiRootPropagation.fromOptionValue(mode.getOptionValue()), equalTo(mode));
    }

    @ParameterizedTest
    @EnumSource(GenAiRootPropagation.class)
    void fromOptionValue_ignoresCase(final GenAiRootPropagation mode) {
        assertThat(GenAiRootPropagation.fromOptionValue(mode.getOptionValue().toUpperCase()), equalTo(mode));
    }

    @ParameterizedTest
    @EnumSource(GenAiRootPropagation.class)
    void toString_isTheOptionValue(final GenAiRootPropagation mode) {
        assertThat(mode.toString(), equalTo(mode.getOptionValue()));
    }

    @Test
    void getOptionValue_matchesTheDocumentedOptions() {
        assertThat(GenAiRootPropagation.AI_ROOT.getOptionValue(), equalTo("ai_root"));
        assertThat(GenAiRootPropagation.ALL_ROOTS.getOptionValue(), equalTo("all_roots"));
        assertThat(GenAiRootPropagation.NONE.getOptionValue(), equalTo("none"));
    }

    @Test
    void fromOptionValue_withNull_returnsNull() {
        assertThat(GenAiRootPropagation.fromOptionValue(null), nullValue());
    }

    @Test
    void fromOptionValue_withAnUnknownOption_throwsWithTheAllowedValues() {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> GenAiRootPropagation.fromOptionValue("root"));

        assertThat(exception.getMessage(), containsString("'root'"));
        assertThat(exception.getMessage(), containsString("ai_root, all_roots, none"));
    }
}
