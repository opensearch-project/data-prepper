/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VersionUtilTest {

    @ParameterizedTest
    @ArgumentsSource(VersionUtilTestCases.class)
    void test_compareVersions(String version1, String version2, int expected) {
        assertThat(VersionUtil.compareVersions(version1, version2), is(expected));
    }

    @Test
    void test_invalidVersion_throws() {
        assertThrows(NumberFormatException.class, () -> VersionUtil.compareVersions("1.0a", "1.0"));
    }

    static class VersionUtilTestCases implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
            return Stream.of(
                    Arguments.of("1.0.0", "1.0.0", 0),
                    Arguments.of("1.0.0", "1.0.1", -1),
                    Arguments.of("1.0.1", "1.0.0", 1),
                    Arguments.of("1.0.0", "1.0", 0),
                    Arguments.of("1.0.1", "1.0", 1),
                    Arguments.of("1.0.1", "2.0", -1)
            );
        }
    }
}
