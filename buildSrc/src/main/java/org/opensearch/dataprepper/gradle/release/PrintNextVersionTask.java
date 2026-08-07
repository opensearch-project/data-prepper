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
import org.gradle.api.tasks.TaskAction;

/**
 * Gradle task that prints the next release version based on the current version in gradle.properties.
 */
public class PrintNextVersionTask extends DefaultTask {
    
    /**
     * Executes the task to determine and print the next version.
     */
    @TaskAction
    public void printNextVersion() {
        final String currentVersion = getProject().getVersion().toString();
        final String nextVersion = VersionCalculator.determineNextVersion(currentVersion);
        System.out.println(nextVersion);
    }
}
