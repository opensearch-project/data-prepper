/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.opensearch.dataprepper.plugins.source.rds.configuration.ExportConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.TableFilterConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class RdsSourceConfigTest {
    
    private RdsSourceConfig objectUnderTest;
    
    @BeforeEach
    void setup() {
        objectUnderTest = createObjectUnderTest();
    }

    @Test
    void test_default_RdsSourceConfig_returns_default_values() {
        assertThat(objectUnderTest.isAcknowledgmentsEnabled(), equalTo(true));
        assertThat(objectUnderTest.isDisableS3ReadForLeader(), equalTo(false));
    }

    @Test
    void test_when_export_is_not_configured_then_isExportEnabled_returns_false() {
        assertThat(objectUnderTest.isExportEnabled(), equalTo(false));
    }

    @Test
    void test_when_export_is_configured_then_isExportEnabled_returns_true() throws NoSuchFieldException, IllegalAccessException {
        ExportConfig exportConfig = new ExportConfig();
        setField(ExportConfig.class, exportConfig, "kmsKeyId", UUID.randomUUID().toString());
        setField(RdsSourceConfig.class, objectUnderTest, "exportConfig", exportConfig);

        assertThat(objectUnderTest.isExportEnabled(), equalTo(true));
    }

    @ParameterizedTest
    @ArgumentsSource(MySqlTableFilterTestArgumentsProvider.class)
    void test_filter_mysql(List<String> includeTables, List<String> excludeTables, Set<String> expectedTables) throws NoSuchFieldException, IllegalAccessException {
        final Set<String> allTables = new HashSet<>(Set.of("database1.table1", "database1.table2", "database1.table3"));
        TableFilterConfig tableFilterConfig = new TableFilterConfig();
        setField(RdsSourceConfig.class, objectUnderTest, "database", "database1");
        setField(RdsSourceConfig.class, objectUnderTest, "tableFilterConfig", tableFilterConfig);
        setField(TableFilterConfig.class, tableFilterConfig, "include", includeTables);
        setField(TableFilterConfig.class, tableFilterConfig, "exclude", excludeTables);

        objectUnderTest.applyTableFilter(allTables);

        assertThat(expectedTables, is(allTables));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresTableFilterTestArgumentsProvider.class)
    void test_filter_postgres(List<String> includeTables, List<String> excludeTables, Set<String> expectedTables) throws NoSuchFieldException, IllegalAccessException {
        final Set<String> allTables = new HashSet<>(Set.of("database1.schema1.table1", "database1.schema1.table2", "database1.schema1.table3"));
        TableFilterConfig tableFilterConfig = new TableFilterConfig();
        setField(RdsSourceConfig.class, objectUnderTest, "database", "database1");
        setField(RdsSourceConfig.class, objectUnderTest, "tableFilterConfig", tableFilterConfig);
        setField(TableFilterConfig.class, tableFilterConfig, "include", includeTables);
        setField(TableFilterConfig.class, tableFilterConfig, "exclude", excludeTables);

        objectUnderTest.applyTableFilter(allTables);

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

    private RdsSourceConfig createObjectUnderTest() {
        return new RdsSourceConfig();
    }
}
