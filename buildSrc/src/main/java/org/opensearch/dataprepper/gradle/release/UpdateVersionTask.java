/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.gradle.release;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Gradle task that updates the version in gradle.properties to the next release version.
 */
public class UpdateVersionTask extends DefaultTask {
    
    @TaskAction
    public void updateVersion() {
        final String currentVersion = getProject().getVersion().toString();
        final String nextVersion = VersionCalculator.determineNextVersion(currentVersion);
        final Path propertiesFile = getProject().getRootDir().toPath().resolve("gradle.properties");
        
        try {
            VersionUpdater.updateVersionInFile(propertiesFile, nextVersion);
            getLogger().lifecycle("Updated version from {} to {}", currentVersion, nextVersion);
        } catch (IOException e) {
            throw new GradleException("Failed to update gradle.properties", e);
        }
    }
}
