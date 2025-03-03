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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TableFilterConfigHelperTest {
    @Mock
    private TableFilterConfig tableFilterConfig;

    @ParameterizedTest
    @ArgumentsSource(MySqlTableFilterTestArgumentsProvider.class)
    void test_filter_mysql(List<String> includeTables, List<String> excludeTables, Set<String> expectedTables) {
        final Set<String> allTables = new HashSet<>(Set.of("database1.table1", "database1.table2", "database1.table3"));
        lenient().when(tableFilterConfig.getDatabase()).thenReturn("database1");
        when(tableFilterConfig.getInclude()).thenReturn(includeTables);
        when(tableFilterConfig.getExclude()).thenReturn(excludeTables);

        TableFilterConfigHelper.applyTableFilter(allTables, tableFilterConfig);

        assertThat(expectedTables, is(allTables));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresTableFilterTestArgumentsProvider.class)
    void test_filter_postgres(List<String> includeTables, List<String> excludeTables, Set<String> expectedTables) {
        final Set<String> allTables = new HashSet<>(Set.of("database1.schema1.table1", "database1.schema1.table2", "database1.schema1.table3"));
        lenient().when(tableFilterConfig.getDatabase()).thenReturn("database1");
        when(tableFilterConfig.getInclude()).thenReturn(includeTables);
        when(tableFilterConfig.getExclude()).thenReturn(excludeTables);

        TableFilterConfigHelper.applyTableFilter(allTables, tableFilterConfig);

        assertThat(expectedTables, is(allTables));
    }

    static class MySqlTableFilterTestArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
            return Stream.of(
                    // Include only
                    Arguments.of(List.of("table1", "table2"), List.of(), Set.of("database1.table1", "database1.table2")),
                    Arguments.of(List.of("table1", "table4"), List.of(), Set.of("database1.table1")),

                    // Exclude only
                    Arguments.of(List.of(), List.of("table1", "table2"), Set.of("database1.table3")),
                    Arguments.of(List.of(), List.of("table1", "table4"), Set.of("database1.table2", "database1.table3")),

                    // Both include and exclude
                    Arguments.of(List.of("table1", "table2"), List.of("table1", "table2"), Set.of()),
                    Arguments.of(List.of("table1", "table2"), List.of("table2", "table3"), Set.of("database1.table1")),

                    // No include or exclude
                    Arguments.of(List.of(), List.of(), Set.of("database1.table1", "database1.table2", "database1.table3"))
            );
        }
    }

    static class PostgresTableFilterTestArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
            return Stream.of(
                    // Include only
                    Arguments.of(List.of("schema1.table1", "schema1.table2"), List.of(), Set.of("database1.schema1.table1", "database1.schema1.table2")),
                    Arguments.of(List.of("schema1.table1", "schema1.table4"), List.of(), Set.of("database1.schema1.table1")),

                    // Exclude only
                    Arguments.of(List.of(), List.of("schema1.table1", "schema1.table2"), Set.of("database1.schema1.table3")),
                    Arguments.of(List.of(), List.of("schema1.table1", "schema1.table4"), Set.of("database1.schema1.table2", "database1.schema1.table3")),

                    // Both include and exclude
                    Arguments.of(List.of("schema1.table1", "schema1.table2"), List.of("schema1.table1", "schema1.table2"), Set.of()),
                    Arguments.of(List.of("schema1.table1", "schema1.table2"), List.of("schema1.table2", "schema1.table3", "schema2.table2"), Set.of("database1.schema1.table1")),

                    // No include or exclude
                    Arguments.of(List.of(), List.of(), Set.of("database1.schema1.table1", "database1.schema1.table2", "database1.schema1.table3"))
            );
        }
    }
}
