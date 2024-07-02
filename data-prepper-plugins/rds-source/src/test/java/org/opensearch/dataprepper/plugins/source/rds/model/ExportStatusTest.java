/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class ExportStatusTest {

    @ParameterizedTest
    @EnumSource(ExportStatus.class)
    void fromString_returns_expected_value(final ExportStatus status) {
        assertThat(ExportStatus.fromString(status.name()), equalTo(status));
    }

    @ParameterizedTest
    @ArgumentsSource(ProvideTerminalStatusTestData.class)
    void test_is_terminal_returns_expected_result(final String status, final boolean expected_result) {
        assertThat(ExportStatus.isTerminal(status), equalTo(expected_result));
    }

    static class ProvideTerminalStatusTestData implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of("COMPLETE", true),
                    Arguments.of("CANCELED", true),
                    Arguments.of("FAILED", true),
                    Arguments.of("CANCELING", false),
                    Arguments.of("IN_PROGRESS", false),
                    Arguments.of("STARTING", false),
                    Arguments.of("INVALID_STATUS", false),
                    Arguments.of(null, false)
            );
        }
    }
}
