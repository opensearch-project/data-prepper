/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TableFilterConfigHelperTest {
    @Mock
    private TableFilterConfig tableFilterConfig;

    @ParameterizedTest
    @ArgumentsSource(TableFilterTestArgumentsProvider.class)
    void test_with_include_only(List<String> includeTables, List<String> excludeTables, Set<String> expectedTables) {
        final Set<String> allTables = new HashSet<>(Set.of("table1", "table2", "table3"));
        when(tableFilterConfig.getInclude()).thenReturn(includeTables);
        when(tableFilterConfig.getExclude()).thenReturn(excludeTables);

        TableFilterConfigHelper.applyTableFilter(allTables, tableFilterConfig);

        assertThat(expectedTables, is(allTables));
    }

    static class TableFilterTestArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
            Set<String> allTables = new HashSet<>(Set.of("table1", "table2", "table3"));
            return Stream.of(
                    // Include only
                    Arguments.of(List.of("table1", "table2"), List.of(), Set.of("table1", "table2")),
                    Arguments.of(List.of("table1", "table4"), List.of(), Set.of("table1")),

                    // Exclude only
                    Arguments.of(List.of(), List.of("table1", "table2"), Set.of("table3")),
                    Arguments.of(List.of(), List.of("table1", "table4"), Set.of("table2", "table3")),

                    // Both include and exclude
                    Arguments.of(List.of("table1", "table2"), List.of("table1", "table2"), Set.of()),
                    Arguments.of(List.of("table1", "table2"), List.of("table2", "table3"), Set.of("table1")),

                    // No include or exclude
                    Arguments.of(List.of(), List.of(), allTables)
            );
        }
    }
}
