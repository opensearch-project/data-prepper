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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.osgi.framework.BundleException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for BundleAdapter's import-package version range generation (WP-B).
 */
class BundleAdapterVersionRangeTest {

    private FelixPluginManager felixPluginManager;
    private BundleAdapter bundleAdapter;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        bundleAdapter = new BundleAdapter(felixPluginManager.getBundleContext());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void buildVersionedImportPackage_adds_version_range_to_api_packages() {
        final List<String> packages = new ArrayList<>(DataPrepperOsgiPackages.SHARED_API_PACKAGES);

        final String result = BundleAdapter.buildVersionedImportPackage(packages);
        final String apiVersion = DataPrepperApiVersion.resolveVersion();
        final String expectedRange = DataPrepperApiVersion.computeImportRange(apiVersion);

        for (final String pkg : DataPrepperOsgiPackages.SHARED_API_PACKAGES) {
            assertThat("API package should have version range: " + pkg,
                    result, containsString(pkg + ";version=\"" + expectedRange + "\""));
        }
    }

    @Test
    void buildVersionedImportPackage_does_not_add_range_to_slf4j() {
        final List<String> packages = new ArrayList<>(DataPrepperOsgiPackages.SHARED_API_PACKAGES);
        packages.add(DataPrepperOsgiPackages.SLF4J_PACKAGE);

        final String result = BundleAdapter.buildVersionedImportPackage(packages);

        // slf4j should appear without a version range
        assertThat(result, containsString(DataPrepperOsgiPackages.SLF4J_PACKAGE));
        assertThat("slf4j should not have version range",
                result, not(containsString(DataPrepperOsgiPackages.SLF4J_PACKAGE + ";version=")));
    }

    @Test
    void buildVersionedImportPackage_handles_empty_list() {
        final String result = BundleAdapter.buildVersionedImportPackage(Collections.emptyList());
        assertThat(result, is(""));
    }

    @Test
    void buildVersionedImportPackage_handles_non_api_packages() {
        final List<String> packages = Arrays.asList("com.example.custom", "org.other.pkg");

        final String result = BundleAdapter.buildVersionedImportPackage(packages);

        // Non-API packages should not have version ranges
        assertThat(result, containsString("com.example.custom"));
        assertThat(result, not(containsString("com.example.custom;version=")));
        assertThat(result, containsString("org.other.pkg"));
        assertThat(result, not(containsString("org.other.pkg;version=")));
    }

    @Test
    void buildVersionedImportPackage_version_range_is_semver_compatible() {
        final List<String> packages = List.of("org.opensearch.dataprepper.model.event");

        final String result = BundleAdapter.buildVersionedImportPackage(packages);

        // The range should be in [x.y.z,(x+1).0.0) format
        assertTrue(result.matches(".*version=\"\\[\\d+\\.\\d+\\.\\d+,\\d+\\.0\\.0\\)\".*"),
                "Version range should be semver-compatible: " + result);
    }

    @Test
    void createAdaptedBundle_import_package_contains_version_ranges() throws IOException {
        final File jar = createPlainJar("version-range-test");
        final File adapted = bundleAdapter.createAdaptedBundle(jar);

        try (java.util.jar.JarFile adaptedJar = new java.util.jar.JarFile(adapted)) {
            final String importPkg = adaptedJar.getManifest().getMainAttributes()
                    .getValue("Import-Package");
            assertThat(importPkg, is(notNullValue()));

            // API packages should have version ranges
            assertThat(importPkg, containsString("org.opensearch.dataprepper.model.event;version=\"["));
            assertThat(importPkg, containsString("org.opensearch.dataprepper.model.record;version=\"["));
        }
    }

    @Test
    void createAdaptedBundle_import_excludes_bundle_provided_packages() throws IOException {
        // Create a JAR that provides org.opensearch.dataprepper.model.event
        final File jar = createJarWithApiPackage("api-provider-test");
        final File adapted = bundleAdapter.createAdaptedBundle(jar);

        try (java.util.jar.JarFile adaptedJar = new java.util.jar.JarFile(adapted)) {
            final String importPkg = adaptedJar.getManifest().getMainAttributes()
                    .getValue("Import-Package");
            assertThat(importPkg, is(notNullValue()));

            // The bundle provides org.opensearch.dataprepper.model.event, so it should NOT import it.
            // Use exact package+semicolon match to avoid false match on event.exceptions subpackage.
            assertFalse(importPkg.contains("org.opensearch.dataprepper.model.event;"),
                    "Bundle-provided package should not be imported: " + importPkg);

            // Subpackages (e.g. event.exceptions) should still be imported since they are distinct
            assertThat(importPkg, containsString("org.opensearch.dataprepper.model.event.exceptions;version=\"["));

            // Other API packages should still be imported with version ranges
            assertThat(importPkg, containsString("org.opensearch.dataprepper.model.record;version=\"["));
        }
    }

    @Test
    void createAdaptedBundle_import_package_does_not_contain_snapshot() throws IOException {
        final File jar = createPlainJar("no-snapshot-test");
        final File adapted = bundleAdapter.createAdaptedBundle(jar);

        try (java.util.jar.JarFile adaptedJar = new java.util.jar.JarFile(adapted)) {
            final String importPkg = adaptedJar.getManifest().getMainAttributes()
                    .getValue("Import-Package");
            assertThat(importPkg, is(notNullValue()));
            assertFalse(importPkg.contains("SNAPSHOT"),
                    "Import-Package should not contain SNAPSHOT: " + importPkg);
        }
    }

    private File createPlainJar(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty JAR with just manifest
        }
        return jar;
    }

    private File createJarWithApiPackage(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Add a class in the org.opensearch.dataprepper.model.event package
            jos.putNextEntry(new ZipEntry("org/opensearch/dataprepper/model/event/CustomEvent.class"));
            jos.write(new byte[]{0});
            jos.closeEntry();
        }
        return jar;
    }
}
