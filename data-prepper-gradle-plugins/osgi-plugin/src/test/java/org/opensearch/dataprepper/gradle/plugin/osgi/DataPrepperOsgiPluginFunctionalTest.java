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

import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Gradle TestKit functional tests for {@link DataPrepperOsgiPlugin}.
 * These tests apply the plugin to a synthetic project and verify behavior
 * end-to-end via a real Gradle build.
 */
class DataPrepperOsgiPluginFunctionalTest {

    @TempDir
    Path projectDir;

    @BeforeEach
    void setUp() throws IOException {
        // Create a minimal settings.gradle
        Files.writeString(projectDir.resolve("settings.gradle"),
                "rootProject.name = 'test-osgi-plugin'\n");
    }

    @Test
    void build_fails_when_plugin_properties_file_is_missing() throws IOException {
        // Create build.gradle that applies the plugin but has no properties file
        final String buildScript = "plugins {\n"
                + "    id 'java'\n"
                + "    id 'org.opensearch.dataprepper.plugin'\n"
                + "}\n"
                + "version = '1.0.0'\n"
                + "repositories {\n"
                + "    mavenCentral()\n"
                + "}\n";
        Files.writeString(projectDir.resolve("build.gradle"), buildScript);

        // Create a minimal Java source so the project is valid
        final Path srcDir = projectDir.resolve("src/main/java/com/example");
        Files.createDirectories(srcDir);
        Files.writeString(srcDir.resolve("Example.java"),
                "package com.example;\npublic class Example {}\n");

        // Create resources directory (empty - no properties file)
        Files.createDirectories(projectDir.resolve("src/main/resources/META-INF"));

        final BuildResult result = GradleRunner.create()
                .withProjectDir(projectDir.toFile())
                .withPluginClasspath()
                .withArguments("jar", "--stacktrace")
                .forwardOutput()
                .buildAndFail();

        assertThat(result.getOutput(), containsString("data-prepper.plugins.properties"));
    }

    @Test
    void build_succeeds_with_valid_properties_file() throws IOException {
        // Create build.gradle that applies the plugin
        final String buildScript = "plugins {\n"
                + "    id 'java'\n"
                + "    id 'org.opensearch.dataprepper.plugin'\n"
                + "}\n"
                + "version = '2.16.0-SNAPSHOT'\n"
                + "repositories {\n"
                + "    mavenCentral()\n"
                + "}\n";
        Files.writeString(projectDir.resolve("build.gradle"), buildScript);

        // Create a minimal Java source
        final Path srcDir = projectDir.resolve("src/main/java/org/opensearch/dataprepper/plugins/test");
        Files.createDirectories(srcDir);
        Files.writeString(srcDir.resolve("TestPlugin.java"),
                "package org.opensearch.dataprepper.plugins.test;\npublic class TestPlugin {}\n");

        // Create the required properties file
        final Path metaInf = projectDir.resolve("src/main/resources/META-INF");
        Files.createDirectories(metaInf);
        Files.writeString(metaInf.resolve("data-prepper.plugins.properties"),
                "org.opensearch.dataprepper.plugin.packages=org.opensearch.dataprepper.plugins.test\n");

        final BuildResult result = GradleRunner.create()
                .withProjectDir(projectDir.toFile())
                .withPluginClasspath()
                .withArguments("jar", "--stacktrace")
                .forwardOutput()
                .build();

        assertThat(result.task(":jar").getOutcome(), is(TaskOutcome.SUCCESS));
        assertThat(result.getOutput(), containsString("configured bundle"));

        // Verify the JAR was produced and has a manifest with Bundle-SymbolicName
        final File jarFile = projectDir.resolve("build/libs/test-osgi-plugin-2.16.0-SNAPSHOT.jar").toFile();
        assertThat("JAR file should exist", jarFile.exists(), is(true));

        try (java.util.jar.JarFile jar = new java.util.jar.JarFile(jarFile)) {
            final String symbolicName = jar.getManifest().getMainAttributes()
                    .getValue("Bundle-SymbolicName");
            assertThat(symbolicName, is("org.opensearch.dataprepper.plugin.test.osgi.plugin"));
            final String bundleVersion = jar.getManifest().getMainAttributes()
                    .getValue("Bundle-Version");
            assertThat(bundleVersion, is("2.16.0"));
        }
    }

    @Test
    void build_produces_correct_manifest_for_hyphenated_project_name() throws IOException {
        // Override settings to set a hyphenated project name
        Files.writeString(projectDir.resolve("settings.gradle"),
                "rootProject.name = 'otel-trace-source'\n");

        final String buildScript = "plugins {\n"
                + "    id 'java'\n"
                + "    id 'org.opensearch.dataprepper.plugin'\n"
                + "}\n"
                + "version = '3.5.7-beta1'\n"
                + "repositories {\n"
                + "    mavenCentral()\n"
                + "}\n";
        Files.writeString(projectDir.resolve("build.gradle"), buildScript);

        final Path srcDir = projectDir.resolve("src/main/java/org/opensearch/dataprepper/plugins/otel");
        Files.createDirectories(srcDir);
        Files.writeString(srcDir.resolve("OtelPlugin.java"),
                "package org.opensearch.dataprepper.plugins.otel;\npublic class OtelPlugin {}\n");

        final Path metaInf = projectDir.resolve("src/main/resources/META-INF");
        Files.createDirectories(metaInf);
        Files.writeString(metaInf.resolve("data-prepper.plugins.properties"),
                "org.opensearch.dataprepper.plugin.packages=org.opensearch.dataprepper.plugins.otel\n");

        final BuildResult result = GradleRunner.create()
                .withProjectDir(projectDir.toFile())
                .withPluginClasspath()
                .withArguments("jar", "--stacktrace")
                .forwardOutput()
                .build();

        assertThat(result.task(":jar").getOutcome(), is(TaskOutcome.SUCCESS));

        final File jarFile = projectDir.resolve("build/libs/otel-trace-source-3.5.7-beta1.jar").toFile();
        assertThat("JAR file should exist", jarFile.exists(), is(true));

        try (java.util.jar.JarFile jar = new java.util.jar.JarFile(jarFile)) {
            final String symbolicName = jar.getManifest().getMainAttributes()
                    .getValue("Bundle-SymbolicName");
            assertThat(symbolicName, is("org.opensearch.dataprepper.plugin.otel.trace.source"));
            final String bundleVersion = jar.getManifest().getMainAttributes()
                    .getValue("Bundle-Version");
            assertThat(bundleVersion, is("3.5.7.beta1"));
        }
    }

    /**
     * bnd manifest directives apply per clause, so {@code -noimport:=true} must be attached to every
     * exported package glob. When it was appended once to the joined glob list, only the final clause
     * carried it and bnd re-imported the earlier packages that the bundle used internally.
     * <p>
     * bnd consumes {@code -noimport:=true} rather than emitting it, so the directive itself is not
     * observable in the manifest. Its effect is: an exported package used by another package in the
     * same bundle is absent from {@code Import-Package}. Here {@code com.example.beta} references
     * {@code com.example.alpha}, so {@code com.example.alpha} is only kept out of
     * {@code Import-Package} when the first clause also carries the directive.
     */
    @Test
    void build_applies_noimport_to_every_export_clause_when_multiple_packages_are_declared() throws IOException {
        Files.writeString(projectDir.resolve("settings.gradle"),
                "rootProject.name = 'multi-package-plugin'\n");

        final String buildScript = "plugins {\n"
                + "    id 'java'\n"
                + "    id 'org.opensearch.dataprepper.plugin'\n"
                + "}\n"
                + "version = '1.2.3'\n"
                + "repositories {\n"
                + "    mavenCentral()\n"
                + "}\n";
        Files.writeString(projectDir.resolve("build.gradle"), buildScript);

        final Path alphaDir = projectDir.resolve("src/main/java/com/example/alpha");
        Files.createDirectories(alphaDir);
        Files.writeString(alphaDir.resolve("Alpha.java"),
                "package com.example.alpha;\npublic class Alpha {}\n");

        // Beta references Alpha so that bnd would self-import com.example.alpha unless the first
        // export clause also carries -noimport:=true
        final Path betaDir = projectDir.resolve("src/main/java/com/example/beta");
        Files.createDirectories(betaDir);
        Files.writeString(betaDir.resolve("Beta.java"),
                "package com.example.beta;\n"
                        + "import com.example.alpha.Alpha;\n"
                        + "public class Beta {\n"
                        + "    public Alpha newAlpha() {\n"
                        + "        return new Alpha();\n"
                        + "    }\n"
                        + "}\n");

        // A space after the comma exercises the per-entry trimming of the package list
        final Path metaInf = projectDir.resolve("src/main/resources/META-INF");
        Files.createDirectories(metaInf);
        Files.writeString(metaInf.resolve("data-prepper.plugins.properties"),
                "org.opensearch.dataprepper.plugin.packages=com.example.alpha, com.example.beta\n");

        final BuildResult result = GradleRunner.create()
                .withProjectDir(projectDir.toFile())
                .withPluginClasspath()
                .withArguments("jar", "--stacktrace")
                .forwardOutput()
                .build();

        assertThat(result.task(":jar").getOutcome(), is(TaskOutcome.SUCCESS));

        final File jarFile = projectDir.resolve("build/libs/multi-package-plugin-1.2.3.jar").toFile();
        assertThat("JAR file should exist", jarFile.exists(), is(true));

        try (java.util.jar.JarFile jar = new java.util.jar.JarFile(jarFile)) {
            // The Manifest reader unfolds the 72-byte continuation lines, so these values are the
            // logical header values and contain no folding artifacts
            final java.util.jar.Attributes mainAttributes = jar.getManifest().getMainAttributes();
            final String exportPackage = mainAttributes.getValue("Export-Package");
            final String importPackage = mainAttributes.getValue("Import-Package");
            final String pluginClasses = mainAttributes.getValue("DataPrepper-Plugin-Classes");

            // Both declared packages are exported
            assertThat(exportPackage, containsString("com.example.alpha"));
            assertThat(exportPackage, containsString("com.example.beta"));

            // Neither exported package is re-imported, which only holds when every clause carries
            // -noimport:=true. Before the fix only the last clause did, so com.example.alpha appeared here.
            assertThat(importPackage, not(containsString("com.example.alpha")));
            assertThat(importPackage, not(containsString("com.example.beta")));

            // Each entry of the declared package list is trimmed, so no clause starts with whitespace
            assertThat(exportPackage, not(containsString(", com.example.")));
            assertThat(pluginClasses, is("com.example.alpha,com.example.beta"));
        }
    }
}
