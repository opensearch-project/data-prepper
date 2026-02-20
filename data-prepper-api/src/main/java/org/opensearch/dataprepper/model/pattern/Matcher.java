/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.pattern;

/**
 * Interface for regex matcher operations.
 */
public interface Matcher {
    /**
     * Attempts to match the entire region against the pattern.
     *
     * @return true if, and only if, the entire region sequence matches this matcher's pattern
     */
    boolean matches();
    
    /**
     * Attempts to find the next subsequence of the input sequence that matches the pattern.
     *
     * @return true if, and only if, a subsequence of the input sequence matches this matcher's pattern
     */
    boolean find();
    
    /**
     * Returns the input subsequence matched by the previous match.
     *
     * @return The (possibly empty) subsequence matched by the previous match, in string form
     */
    String group();
    
    /**
     * Returns the input subsequence captured by the given group during the previous match operation.
     *
     * @param group The index of a capturing group in this matcher's pattern
     * @return The (possibly empty) subsequence captured by the group during the previous match, or null if the group failed to match part of the input
     */
    String group(int group);
    
    /**
     * Replaces every subsequence of the input sequence that matches the pattern with the given replacement string.
     *
     * @param replacement The replacement string
     * @return The string constructed by replacing each matching subsequence by the replacement string
     */
    String replaceAll(String replacement);
}
