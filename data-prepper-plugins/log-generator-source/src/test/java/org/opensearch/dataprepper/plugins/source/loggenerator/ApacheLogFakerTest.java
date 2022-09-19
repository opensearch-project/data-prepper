/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loggenerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ApacheLogFakerTest {
    public final String EXTENDED_APACHE_LOG_REGEX = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+-]\\d{4})\\] \"(.+?)\" " +
            "(\\d{3}) (\\d+) \"([^\"]+)\" \"(.+?)\"";

    public final String COMMON_APACHE_LOG_REGEX = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+-]\\d{4})\\] \"(.+?)\" " +
            "(\\d{3}) (\\d+)";

    @Test
    public void testRandomExtendedLogPattern() {
        // Given
        ApacheLogFaker objectUnderTest = new ApacheLogFaker();

        // When/Then
        Assertions.assertTrue(objectUnderTest.generateRandomExtendedApacheLog().matches(EXTENDED_APACHE_LOG_REGEX));
    }

    @Test
    public void testRandomCommonLogPattern() {
        // Given
        ApacheLogFaker objectUnderTest = new ApacheLogFaker();

        // When/Then
        Assertions.assertTrue(objectUnderTest.generateRandomCommonApacheLog().matches(COMMON_APACHE_LOG_REGEX));
    }
}