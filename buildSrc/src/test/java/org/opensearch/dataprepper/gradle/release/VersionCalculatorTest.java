/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.gradle.release;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VersionCalculatorTest {
    @ParameterizedTest
    @CsvSource({
        "2.6.2-SNAPSHOT, 2.6.2",
        "1.0.0-SNAPSHOT, 1.0.0",
        "10.20.30-SNAPSHOT, 10.20.30",
        "0.0.1-SNAPSHOT, 0.0.1"
    })
    void determineNextVersion_with_SNAPSHOT_updates_to_same_version_without_SNAPSHOT(final String input, final String expected) {
        final String result = VersionCalculator.determineNextVersion(input);
        assertThat(result, equalTo(expected));
    }
    
    @ParameterizedTest
    @CsvSource({
        "2.6.2, 2.6.3",
        "1.0.0, 1.0.1",
        "10.20.30, 10.20.31",
        "0.0.0, 0.0.1",
        "999.999.999, 999.999.1000"
    })
    void determineNextVersion_with_non_SNAPSHOT_updates_to_next_patch_version(final String input, final String expected) {
        final String result = VersionCalculator.determineNextVersion(input);
        assertThat(result, equalTo(expected));
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "invalid",
        "1.2",
        "1.2.3.4",
        "1.2.3-BETA",
        "a.b.c",
        "1.2.x",
        "",
        "1.2.3-",
        "-SNAPSHOT"
    })
    void determineNextVersion_with_invalid_throws(String invalidVersion) {
        assertThrows(IllegalArgumentException.class, () -> {
            VersionCalculator.determineNextVersion(invalidVersion);
        });
    }
    
    @Test
    void determineNextVersion_with_null_throws() {
        assertThrows(NullPointerException.class, () -> {
            VersionCalculator.determineNextVersion(null);
        });
    }
}
