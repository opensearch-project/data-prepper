/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.regex;

import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RegexValueValidator {
    public static boolean validateRegex(final String pattern) {
        if (pattern != null && !Objects.equals(pattern, "")) {
            try {
                Pattern.compile(pattern);
            } catch (PatternSyntaxException e) {
                return false;
            }
        }

        return true;
    }
}
