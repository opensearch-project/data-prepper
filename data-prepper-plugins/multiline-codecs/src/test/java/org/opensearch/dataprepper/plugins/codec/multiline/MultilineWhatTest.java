/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec.multiline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MultilineWhatTest {

    @Test
    void fromString_returns_PREVIOUS_for_previous() {
        assertThat(MultilineWhat.fromString("previous"), equalTo(MultilineWhat.PREVIOUS));
    }

    @Test
    void fromString_returns_NEXT_for_next() {
        assertThat(MultilineWhat.fromString("next"), equalTo(MultilineWhat.NEXT));
    }

    @Test
    void fromString_is_case_insensitive() {
        assertThat(MultilineWhat.fromString("PREVIOUS"), equalTo(MultilineWhat.PREVIOUS));
        assertThat(MultilineWhat.fromString("NEXT"), equalTo(MultilineWhat.NEXT));
        assertThat(MultilineWhat.fromString("Previous"), equalTo(MultilineWhat.PREVIOUS));
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "before", "after", ""})
    void fromString_throws_for_invalid_value(final String value) {
        assertThrows(IllegalArgumentException.class, () -> MultilineWhat.fromString(value));
    }

    @Test
    void toString_returns_correct_values() {
        assertThat(MultilineWhat.PREVIOUS.toString(), equalTo("previous"));
        assertThat(MultilineWhat.NEXT.toString(), equalTo("next"));
    }
}
