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

import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataPrepperOsgiPackagesTest {

    @Test
    void buildSystemPackagesExtra_contains_all_api_packages() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();

        for (final String pkg : DataPrepperOsgiPackages.SHARED_API_PACKAGES) {
            assertThat("Missing API package: " + pkg, result, containsString(pkg));
        }
    }

    @Test
    void buildSystemPackagesExtra_contains_slf4j_package() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        assertThat(result, containsString(DataPrepperOsgiPackages.SLF4J_PACKAGE));
    }

    @Test
    void buildSystemPackagesExtra_api_packages_have_version_attribute() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        final String resolvedVersion = DataPrepperApiVersion.resolveVersion();

        for (final String pkg : DataPrepperOsgiPackages.SHARED_API_PACKAGES) {
            final String expectedEntry = pkg + ";version=\"" + resolvedVersion + "\"";
            assertThat("API package should be versioned: " + pkg, result, containsString(expectedEntry));
        }
    }

    @Test
    void buildSystemPackagesExtra_slf4j_does_not_have_api_version() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        final String resolvedVersion = DataPrepperApiVersion.resolveVersion();

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
    void buildSystemPackagesExtra_produces_valid_osgi_format() {
        final String result = DataPrepperOsgiPackages.buildSystemPackagesExtra();
        // Each entry should be parseable: package[;version="x.y.z"]
        final String[] entries = result.split(",");
        assertTrue(entries.length > 0);
        for (final String entry : entries) {
            assertFalse(entry.trim().isEmpty(), "Empty entry found in system packages extra");
        }
    }

    @Test
    void shared_api_packages_is_not_empty() {
        assertFalse(DataPrepperOsgiPackages.SHARED_API_PACKAGES.isEmpty());
    }

    @Test
    void shared_api_packages_contains_event_package() {
        assertTrue(DataPrepperOsgiPackages.SHARED_API_PACKAGES.contains(
                "org.opensearch.dataprepper.model.event"));
    }

    @Test
    void shared_api_packages_contains_all_required_model_packages() {
        final List<String> requiredPackages = List.of(
                "org.opensearch.dataprepper.model.acknowledgements",
                "org.opensearch.dataprepper.model.codec",
                "org.opensearch.dataprepper.model.types",
                "org.opensearch.dataprepper.model.failures",
                "org.opensearch.dataprepper.model.io",
                "org.opensearch.dataprepper.model.encryption",
                "org.opensearch.dataprepper.model.plugin",
                "org.opensearch.dataprepper.model.breaker",
                "org.opensearch.dataprepper.model.constraints",
                "org.opensearch.dataprepper.model.document",
                "org.opensearch.dataprepper.model.host",
                "org.opensearch.dataprepper.model.opensearch",
                "org.opensearch.dataprepper.model.peerforwarder",
                "org.opensearch.dataprepper.model.pipeline",
                "org.opensearch.dataprepper.model.validation",
                "org.opensearch.dataprepper.model.source.coordinator",
                "org.opensearch.dataprepper.model.source.coordinator.enhanced",
                "org.opensearch.dataprepper.model.source.coordinator.exceptions",
                "org.opensearch.dataprepper.model.source.s3"
        );

        for (final String pkg : requiredPackages) {
            assertTrue(DataPrepperOsgiPackages.SHARED_API_PACKAGES.contains(pkg),
                    "Missing required package: " + pkg);
        }
    }

    @Test
    void shared_api_packages_contains_top_level_api_packages() {
        final List<String> requiredTopLevel = List.of(
                "org.opensearch.dataprepper.expression",
                "org.opensearch.dataprepper.logging",
                "org.opensearch.dataprepper.metrics",
                "org.opensearch.dataprepper.typeconverter"
        );

        for (final String pkg : requiredTopLevel) {
            assertTrue(DataPrepperOsgiPackages.SHARED_API_PACKAGES.contains(pkg),
                    "Missing required top-level package: " + pkg);
        }
    }

    @Test
    void shared_api_packages_are_sorted_alphabetically() {
        final List<String> packages = DataPrepperOsgiPackages.SHARED_API_PACKAGES;
        for (int i = 1; i < packages.size(); i++) {
            assertTrue(packages.get(i - 1).compareTo(packages.get(i)) <= 0,
                    "Packages not sorted: " + packages.get(i - 1) + " > " + packages.get(i));
        }
    }
}
