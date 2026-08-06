/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gradle.plugin.osgi;

import org.gradle.testfixtures.ProjectBuilder;
import org.gradle.api.Project;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class DataPrepperOsgiPluginTest {

    @TempDir
    Path tempDir;

    // ========================================================================
    // toOsgiVersion tests
    // ========================================================================

    @ParameterizedTest
    @NullAndEmptySource
    void toOsgiVersion_returns_fallback_for_null_or_empty(final String input) {
        assertThat(DataPrepperOsgiPlugin.toOsgiVersion(input), is(equalTo("0.0.0")));
    }

    @ParameterizedTest
    @CsvSource({
            "2.16.0-SNAPSHOT, 2.16.0",
            "2.16.0,          2.16.0",
            "2.16,            2.16.0",
            "1.0.0,           1.0.0",
            "0.1.2,           0.1.2",
            "10.20.30,        10.20.30",
            "3.5.7-beta1,     3.5.7.beta1",
            "1.2.3-rc.2,      1.2.3.rc2",
            "1.2.3.RELEASE,   1.2.3.RELEASE",
            "5.0-SNAPSHOT,    5.0.0"
    })
    void toOsgiVersion_converts_correctly(final String input, final String expected) {
        assertThat(DataPrepperOsgiPlugin.toOsgiVersion(input), is(equalTo(expected)));
    }

    @Test
    void toOsgiVersion_single_digit_version_returns_fallback() {
        assertThat(DataPrepperOsgiPlugin.toOsgiVersion("7"), is(equalTo("0.0.0")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-a-version", "abc.def.ghi", "v1.2.3"})
    void toOsgiVersion_returns_fallback_for_invalid_format(final String input) {
        assertThat(DataPrepperOsgiPlugin.toOsgiVersion(input), is(equalTo("0.0.0")));
    }

    @Test
    void toOsgiVersion_snapshot_qualifier_is_case_insensitive() {
        assertThat(DataPrepperOsgiPlugin.toOsgiVersion("2.16.0-snapshot"), is(equalTo("2.16.0")));
        assertThat(DataPrepperOsgiPlugin.toOsgiVersion("2.16.0-Snapshot"), is(equalTo("2.16.0")));
    }

    @Test
    void toOsgiVersion_qualifier_with_only_illegal_chars_is_dropped() {
        assertThat(DataPrepperOsgiPlugin.toOsgiVersion("1.2.3---"), is(equalTo("1.2.3")));
    }

    // ========================================================================
    // sanitizeName tests
    // ========================================================================

    @Test
    void sanitizeName_replaces_hyphens_with_dots() {
        assertThat(DataPrepperOsgiPlugin.sanitizeName("my-plugin-name"), is(equalTo("my.plugin.name")));
    }

    @Test
    void sanitizeName_replaces_underscores_with_dots() {
        assertThat(DataPrepperOsgiPlugin.sanitizeName("my_plugin_name"), is(equalTo("my.plugin.name")));
    }

    @Test
    void sanitizeName_preserves_dots_and_alphanumeric() {
        assertThat(DataPrepperOsgiPlugin.sanitizeName("plugin.v2"), is(equalTo("plugin.v2")));
    }

    @Test
    void sanitizeName_replaces_special_chars() {
        assertThat(DataPrepperOsgiPlugin.sanitizeName("plugin@#$%name"), is(equalTo("plugin....name")));
    }

    @Test
    void sanitizeName_preserves_empty_string() {
        assertThat(DataPrepperOsgiPlugin.sanitizeName(""), is(equalTo("")));
    }

    // ========================================================================
    // computeSymbolicName tests
    // ========================================================================

    @Test
    void computeSymbolicName_prefixes_with_data_prepper_plugin() {
        final Project project = ProjectBuilder.builder()
                .withName("otel-trace-source")
                .withProjectDir(tempDir.resolve("proj1").toFile())
                .build();
        final String symbolicName = DataPrepperOsgiPlugin.computeSymbolicName(project);
        assertThat(symbolicName, is(equalTo("org.opensearch.dataprepper.plugin.otel.trace.source")));
    }

    @Test
    void computeSymbolicName_handles_simple_name() {
        final Project project = ProjectBuilder.builder()
                .withName("simple")
                .withProjectDir(tempDir.resolve("proj2").toFile())
                .build();
        final String symbolicName = DataPrepperOsgiPlugin.computeSymbolicName(project);
        assertThat(symbolicName, is(equalTo("org.opensearch.dataprepper.plugin.simple")));
    }

    @Test
    void computeSymbolicName_handles_name_with_underscores_and_numbers() {
        final Project project = ProjectBuilder.builder()
                .withName("s3_sink_v2")
                .withProjectDir(tempDir.resolve("proj3").toFile())
                .build();
        final String symbolicName = DataPrepperOsgiPlugin.computeSymbolicName(project);
        assertThat(symbolicName, is(equalTo("org.opensearch.dataprepper.plugin.s3.sink.v2")));
    }
}
