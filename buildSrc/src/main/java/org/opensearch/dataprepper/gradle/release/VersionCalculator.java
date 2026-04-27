/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.gradle.release;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for calculating the next release version from a current version string.
 */
class VersionCalculator {
    private static final Pattern VERSION_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)(-SNAPSHOT)?$");
    
    /**
     * Determines the next release version from the current version.
     * <p>
     * If the current version ends with "-SNAPSHOT", the suffix is removed.
     * If the current version does not end with "-SNAPSHOT", the patch version is incremented by 1.
     * 
     * @param currentVersion The current version string (e.g., "2.6.2-SNAPSHOT" or "2.6.2")
     * @return The next release version (e.g., "2.6.2" or "2.6.3")
     * @throws IllegalArgumentException if version format is invalid
     * @throws NullPointerException if currentVersion is null
     */
    static String determineNextVersion(String currentVersion) {
        if (currentVersion == null) {
            throw new NullPointerException("currentVersion cannot be null");
        }
        
        Matcher matcher = VERSION_PATTERN.matcher(currentVersion);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                "Invalid version format: " + currentVersion + 
                ". Expected format: {major}.{minor}.{patch}[-SNAPSHOT]"
            );
        }
        
        String major = matcher.group(1);
        String minor = matcher.group(2);
        String patch = matcher.group(3);
        String snapshot = matcher.group(4);
        
        // If version has -SNAPSHOT suffix, remove it
        if (snapshot != null) {
            return major + "." + minor + "." + patch;
        }
        
        // Otherwise, increment patch version
        int patchNumber = Integer.parseInt(patch);
        int nextPatch = patchNumber + 1;
        return major + "." + minor + "." + nextPatch;
    }
}
