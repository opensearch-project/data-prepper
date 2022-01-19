/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.integration.log;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ApacheLogFakerTest {
    public final String APACHE_LOG_REGEX = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+-]\\d{4})\\] \"(.+?)\" " +
            "(\\d{3}) (\\d+) \"([^\"]+)\" \"(.+?)\"";

    @Test
    public void testRandomLogPattern() {
        // Given
        ApacheLogFaker objectUnderTest = new ApacheLogFaker();

        // When/Then
        Assertions.assertTrue(objectUnderTest.generateRandomApacheLog().matches(APACHE_LOG_REGEX));
    }
}