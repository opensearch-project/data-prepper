/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.pattern;

import java.util.ServiceLoader;

/**
 * Service provider interface for Pattern implementations.
 * Implementations should be registered via Java ServiceLoader mechanism.
 */
public interface PatternProvider {
    /**
     * Compiles the given regular expression into a pattern.
     *
     * @param regex The expression to be compiled
     * @return the compiled pattern
     * @throws PatternSyntaxException if the expression's syntax is invalid
     */
    Pattern compile(String regex) throws PatternSyntaxException;
    
    /**
     * Returns the name of this provider (e.g., "java", "re2j").
     */
    String getName();
    
    /**
     * Gets the pattern provider instance based on system property.
     * Uses "dataprepper.pattern.provider" system property to select implementation.
     * Defaults to Java SDK Pattern if property not set or provider not found.
     *
     * @return the pattern provider
     */
    static PatternProvider getProvider() {
        String providerName = System.getProperty("dataprepper.pattern.provider", "java");
        
        ServiceLoader<PatternProvider> loader = ServiceLoader.load(PatternProvider.class);
        for (PatternProvider provider : loader) {
            if (provider.getName().equalsIgnoreCase(providerName)) {
                return provider;
            }
        }
        
        // Default to Java SDK Pattern
        return new JavaPatternProvider();
    }
}
