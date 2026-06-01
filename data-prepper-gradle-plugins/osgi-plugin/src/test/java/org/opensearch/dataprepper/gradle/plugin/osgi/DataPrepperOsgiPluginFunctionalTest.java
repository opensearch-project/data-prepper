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
                + "    id 'org.opensearch.dataprepper.osgi'\n"
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
                + "    id 'org.opensearch.dataprepper.osgi'\n"
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
                + "    id 'org.opensearch.dataprepper.osgi'\n"
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
}
