/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.pattern;

/**
 * Default pattern provider using Java SDK Pattern.
 */
class JavaPatternProvider implements PatternProvider {
    @Override
    public String getName() {
        return "java";
    }
    
    @Override
    public Pattern compile(String regex) throws PatternSyntaxException {
        try {
            return new JavaPattern(java.util.regex.Pattern.compile(regex));
        } catch (java.util.regex.PatternSyntaxException e) {
            throw new PatternSyntaxException(e.getMessage(), regex, e);
        }
    }
    
    private static class JavaPattern implements Pattern {
        private final java.util.regex.Pattern pattern;
        
        JavaPattern(java.util.regex.Pattern pattern) {
            this.pattern = pattern;
        }
        
        @Override
        public Matcher matcher(CharSequence input) {
            return new JavaMatcher(pattern.matcher(input));
        }
    }
    
    private static class JavaMatcher implements Matcher {
        private final java.util.regex.Matcher matcher;
        
        JavaMatcher(java.util.regex.Matcher matcher) {
            this.matcher = matcher;
        }
        
        @Override
        public boolean matches() {
            return matcher.matches();
        }
        
        @Override
        public boolean find() {
            return matcher.find();
        }
        
        @Override
        public String group() {
            return matcher.group();
        }
        
        @Override
        public String group(int group) {
            return matcher.group(group);
        }
        
        @Override
        public String replaceAll(String replacement) {
            return matcher.replaceAll(replacement);
        }
    }
}
