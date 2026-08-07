/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.gradle.release;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class VersionUpdaterTest {
    
    @TempDir
    Path tempDir;

    private Path propertiesFile;

    @BeforeEach
    void setUp() {
        propertiesFile = tempDir.resolve("gradle.properties");
    }
    
    @Test
    void updateVersionInFile_updates_version_line() throws IOException {

        Files.write(propertiesFile, List.of(
            "# Comment",
            "version=1.0.0-SNAPSHOT",
            "org.gradle.jvmargs=-Xmx2048M"
        ));
        
        VersionUpdater.updateVersionInFile(propertiesFile, "1.0.0");
        
        final List<String> lines = Files.readAllLines(propertiesFile);
        assertThat(lines.get(0), equalTo("# Comment"));
        assertThat(lines.get(1), equalTo("version=1.0.0"));
        assertThat(lines.get(2), equalTo("org.gradle.jvmargs=-Xmx2048M"));
    }
    
    @Test
    void updateVersionInFile_preserves_other_lines() throws IOException {
        Files.write(propertiesFile, List.of(
            "someProperty=value",
            "version=2.5.0",
            "anotherProperty=anotherValue"
        ));
        
        VersionUpdater.updateVersionInFile(propertiesFile, "2.6.0");
        
        final List<String> lines = Files.readAllLines(propertiesFile);
        assertThat(lines.get(0), equalTo("someProperty=value"));
        assertThat(lines.get(1), equalTo("version=2.6.0"));
        assertThat(lines.get(2), equalTo("anotherProperty=anotherValue"));
    }
}
