/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.pattern;

/**
 * Exception thrown when a syntax error is encountered in a pattern.
 */
public class PatternSyntaxException extends RuntimeException {
    private final String pattern;
    
    public PatternSyntaxException(String message, String pattern) {
        super(message);
        this.pattern = pattern;
    }
    
    public PatternSyntaxException(String message, String pattern, Throwable cause) {
        super(message, cause);
        this.pattern = pattern;
    }
    
    public String getPattern() {
        return pattern;
    }
}
