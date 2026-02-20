/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.pattern;

/**
 * Pattern provider using Re2J regex engine for protection against catastrophic backtracking.
 */
class Re2JPatternProvider implements PatternProvider {
    @Override
    public String getName() {
        return "re2j";
    }
    
    @Override
    public Pattern compile(String regex) throws PatternSyntaxException {
        try {
            return new Re2JPattern(com.google.re2j.Pattern.compile(regex));
        } catch (com.google.re2j.PatternSyntaxException e) {
            throw new PatternSyntaxException(e.getMessage(), regex, e);
        }
    }
    
    private static class Re2JPattern implements Pattern {
        private final com.google.re2j.Pattern pattern;
        
        Re2JPattern(com.google.re2j.Pattern pattern) {
            this.pattern = pattern;
        }
        
        @Override
        public Matcher matcher(CharSequence input) {
            return new Re2JMatcher(pattern.matcher(input));
        }
    }
    
    private static class Re2JMatcher implements Matcher {
        private final com.google.re2j.Matcher matcher;
        
        Re2JMatcher(com.google.re2j.Matcher matcher) {
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
