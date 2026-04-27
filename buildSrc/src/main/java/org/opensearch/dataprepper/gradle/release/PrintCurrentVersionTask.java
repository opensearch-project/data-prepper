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
 * Gradle task that prints the current version from gradle.properties.
 */
public class PrintCurrentVersionTask extends DefaultTask {
    
    @TaskAction
    public void printCurrentVersion() {
        System.out.println(getProject().getVersion().toString());
    }
}
