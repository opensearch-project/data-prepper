package org.opensearch.dataprepper.validations;

import io.krakens.grok.api.GrokCompiler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class GrokPatternValidator {

    private static final GrokCompiler grokCompiler = GrokCompiler.newInstance();

    static {
        grokCompiler.registerDefaultPatterns();
    }

    private GrokPatternValidator() {

    }

    public static List<String> validatePatterns(final Map<String, String> additionalPatternsToRegister, final List<String> matchPatterns) {
        final List<String> incompatiblePatternErrors = new ArrayList<>();
        additionalPatternsToRegister.forEach((patternName, pattern) -> {
            try {
                grokCompiler.register(patternName, pattern);
            } catch (final NullPointerException | IllegalArgumentException e) {
                final String errorMessage = String.format("The grok pattern with name \"%s\" and pattern \"%s\" is invalid with the following error: %s", patternName, pattern, e.getMessage());
                incompatiblePatternErrors.add(errorMessage);
            }
        });

        if (incompatiblePatternErrors.isEmpty()) {
            matchPatterns.forEach(pattern -> {
                try {
                    grokCompiler.compile(pattern);
                } catch (final IllegalArgumentException e) {
                    final String errorMessage = String.format("The grok match pattern \"%s\" is invalid with the following error: %s", pattern, e.getMessage());
                    incompatiblePatternErrors.add(errorMessage);
                }
            });
        }

        return incompatiblePatternErrors;
    }

}
