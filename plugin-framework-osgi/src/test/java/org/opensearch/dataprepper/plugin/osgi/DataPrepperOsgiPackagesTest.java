/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataPrepperOsgiPackagesTest {

    @Test
    void loadDataPrepperPackages_reads_from_generated_resource() {
        final List<String> packages = DataPrepperOsgiPackages.loadDataPrepperPackages();
        assertFalse(packages.isEmpty(), "Should load packages from generated resource");
    }

    @Test
    void loadDataPrepperPackages_includes_model_event_package() {
        final List<String> packages = DataPrepperOsgiPackages.loadDataPrepperPackages();
        assertTrue(packages.contains("org.opensearch.dataprepper.model.event"),
                "Should include org.opensearch.dataprepper.model.event");
    }

    @Test
    void loadDataPrepperPackages_includes_model_subpackages() {
        final List<String> packages = DataPrepperOsgiPackages.loadDataPrepperPackages();
        assertTrue(packages.contains("org.opensearch.dataprepper.model.buffer"),
                "Should include model.buffer");
        assertTrue(packages.contains("org.opensearch.dataprepper.model.processor"),
                "Should include model.processor");
        assertTrue(packages.contains("org.opensearch.dataprepper.model.sink"),
                "Should include model.sink");
        assertTrue(packages.contains("org.opensearch.dataprepper.model.source"),
                "Should include model.source");
        assertTrue(packages.contains("org.opensearch.dataprepper.model.configuration"),
                "Should include model.configuration");
    }

    @Test
    void loadDataPrepperPackages_includes_top_level_api_packages() {
        final List<String> packages = DataPrepperOsgiPackages.loadDataPrepperPackages();
        assertTrue(packages.contains("org.opensearch.dataprepper.expression"),
                "Should include expression");
        assertTrue(packages.contains("org.opensearch.dataprepper.metrics"),
                "Should include metrics");
    }

    @Test
    void loadDataPrepperPackages_excludes_plugins_namespace() {
        final List<String> packages = DataPrepperOsgiPackages.loadDataPrepperPackages();
        for (final String pkg : packages) {
            assertFalse(pkg.startsWith("org.opensearch.dataprepper.plugins"),
                    "Should NOT include plugins namespace: " + pkg);
        }
    }

    @Test
    void loadThirdPartyPackages_includes_jackson_annotation() {
        final List<String> packages = DataPrepperOsgiPackages.loadThirdPartyPackages();
        assertTrue(packages.contains("com.fasterxml.jackson.annotation"),
                "Should include Jackson annotation package");
    }

    @Test
    void loadThirdPartyPackages_includes_jakarta_validation() {
        final List<String> packages = DataPrepperOsgiPackages.loadThirdPartyPackages();
        assertTrue(packages.contains("jakarta.validation"),
                "Should include jakarta.validation");
        assertTrue(packages.contains("jakarta.validation.constraints"),
                "Should include jakarta.validation.constraints");
    }

    @Test
    void loadThirdPartyPackages_does_not_include_jackson_databind() {
        final List<String> packages = DataPrepperOsgiPackages.loadThirdPartyPackages();
        assertFalse(packages.contains("com.fasterxml.jackson.databind"),
                "Should NOT include jackson databind");
    }

    @Test
    void loadThirdPartyPackages_does_not_include_jackson_core() {
        final List<String> packages = DataPrepperOsgiPackages.loadThirdPartyPackages();
        assertFalse(packages.contains("com.fasterxml.jackson.core"),
                "Should NOT include jackson core");
    }

    @Test
    void buildSystemPackagesExtra_contains_data_prepper_packages() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();

        assertThat("Should include model.event", result,
                containsString("org.opensearch.dataprepper.model.event"));
        assertThat("Should include model.processor", result,
                containsString("org.opensearch.dataprepper.model.processor"));
    }

    @Test
    void buildSystemPackagesExtra_contains_slf4j_package() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        assertThat(result, containsString(DataPrepperOsgiPackages.SLF4J_PACKAGE));
    }

    @Test
    void buildSystemPackagesExtra_contains_framework_package() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        assertThat(result, containsString(DataPrepperOsgiPackages.FRAMEWORK_PACKAGE));
    }

    @Test
    void buildSystemPackagesExtra_contains_jackson_annotation() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        assertThat(result, containsString("com.fasterxml.jackson.annotation"));
    }

    @Test
    void buildSystemPackagesExtra_contains_jakarta_validation() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        assertThat(result, containsString("jakarta.validation"));
        assertThat(result, containsString("jakarta.validation.constraints"));
    }

    @Test
    void buildSystemPackagesExtra_does_not_contain_jackson_databind() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        assertThat(result, not(containsString("jackson.databind")));
    }

    @Test
    void buildSystemPackagesExtra_does_not_contain_jackson_core() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        assertThat(result, not(containsString("jackson.core")));
    }

    @Test
    void buildSystemPackagesExtra_api_packages_have_version_attribute() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        final String resolvedVersion = DataPrepperOsgiPackages.resolveOsgiVersion();

        final String expectedEntry = "org.opensearch.dataprepper.model.event;version=\"" + resolvedVersion + "\"";
        assertThat("API packages should be versioned", result, containsString(expectedEntry));
    }

    @Test
    void buildSystemPackagesExtra_slf4j_does_not_have_api_version() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        final String resolvedVersion = DataPrepperOsgiPackages.resolveOsgiVersion();

        // slf4j should NOT have the Data Prepper API version attached
        final String versionedSlf4j = DataPrepperOsgiPackages.SLF4J_PACKAGE + ";version=\"" + resolvedVersion + "\"";
        assertThat("slf4j should not have Data Prepper API version",
                result, not(containsString(versionedSlf4j)));
    }

    @Test
    void buildSystemPackagesExtra_does_not_contain_snapshot() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        assertFalse(result.contains("SNAPSHOT"),
                "System packages extra should not contain SNAPSHOT: " + result);
    }

    @Test
    void buildSystemPackagesExtra_produces_valid_entries() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        // Each entry should be parseable: package[;version="x.y.z"]
        final String[] entries = result.split(",");
        assertTrue(entries.length > 0);
        for (final String entry : entries) {
            assertFalse(entry.trim().isEmpty(), "Empty entry found in system packages extra");
        }
    }

    @Test
    void resolveOsgiVersion_returns_valid_three_part_version() {
        final String version = DataPrepperOsgiPackages.resolveOsgiVersion();
        assertTrue(version.matches("\\d+\\.\\d+\\.\\d+"),
                "Version should be in major.minor.micro format: " + version);
    }

    @Test
    void resolveOsgiVersion_does_not_contain_snapshot() {
        final String version = DataPrepperOsgiPackages.resolveOsgiVersion();
        assertFalse(version.contains("SNAPSHOT"),
                "OSGi version should not contain SNAPSHOT: " + version);
    }

    @ParameterizedTest
    @CsvSource({
            "2.16.0, 2.16.0",
            "2.16.0-SNAPSHOT, 2.16.0",
            "3.0.0-SNAPSHOT, 3.0.0",
            "2.15, 2.15.0",
            "1.0.0, 1.0.0"
    })
    void toOsgiVersion_converts_correctly(final String input, final String expected) {
        assertThat(DataPrepperOsgiPackages.toOsgiVersion(input),
                org.hamcrest.CoreMatchers.is(expected));
    }

    @ParameterizedTest
    @CsvSource({
            "'', 0.0.0",
            "invalid, 0.0.0"
    })
    void toOsgiVersion_returns_fallback_for_invalid_input(final String input, final String expected) {
        assertThat(DataPrepperOsgiPackages.toOsgiVersion(input),
                org.hamcrest.CoreMatchers.is(expected));
    }

    @Test
    void toOsgiVersion_returns_fallback_for_null() {
        assertThat(DataPrepperOsgiPackages.toOsgiVersion(null),
                org.hamcrest.CoreMatchers.is("0.0.0"));
    }
}
