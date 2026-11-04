/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.pattern;

/**
 * Interface for regex pattern matching operations.
 * Implementations can use different regex engines (Java SDK, Re2J, etc.)
 */
public interface Pattern {
    /**
     * Creates a matcher that will match the given input against this pattern.
     *
     * @param input The character sequence to be matched
     * @return A new matcher for this pattern
     */
    Matcher matcher(CharSequence input);
    
    /**
     * Compiles the given regular expression into a pattern.
     *
     * @param regex The expression to be compiled
     * @return the compiled pattern
     * @throws PatternSyntaxException if the expression's syntax is invalid
     */
    static Pattern compile(String regex) throws PatternSyntaxException {
        return PatternProvider.getProvider().compile(regex);
    }
}
