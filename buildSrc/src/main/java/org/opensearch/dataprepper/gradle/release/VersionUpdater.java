/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.gradle.release;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for updating version in gradle.properties files.
 */
class VersionUpdater {
    
    /**
     * Updates the version line in a gradle.properties file.
     * 
     * @param propertiesFile Path to the gradle.properties file
     * @param newVersion The new version to set
     * @throws IOException if file operations fail
     */
    static void updateVersionInFile(final Path propertiesFile, final String newVersion) throws IOException {
        final List<String> lines = Files.readAllLines(propertiesFile);
        final List<String> updatedLines = lines.stream()
            .map(line -> line.startsWith("version=") ? "version=" + newVersion : line)
            .collect(Collectors.toList());
        
        Files.write(propertiesFile, updatedLines);
    }
}
