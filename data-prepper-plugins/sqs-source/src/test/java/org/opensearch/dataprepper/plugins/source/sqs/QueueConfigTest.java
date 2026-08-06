/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.time.Duration;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueueConfigTest {

    private static final Pattern SQS_URL_PATTERN = Pattern.compile(
            "^https://sqs\\.[a-z0-9-]+\\.amazonaws\\.com(\\.cn)?/\\d{12}/[a-zA-Z0-9_-]+$");

    @Test
    void testDefaultValues() {
        final QueueConfig queueConfig = new QueueConfig();

        assertNull(queueConfig.getUrl(), "URL should be null by default");
        assertEquals(1, queueConfig.getNumWorkers(), "Number of workers should default to 1");
        assertNull(queueConfig.getMaximumMessages(), "Maximum messages should be null by default");
        assertEquals(Duration.ofSeconds(0), queueConfig.getPollDelay(), "Poll delay should default to 0 seconds");
        assertNull(queueConfig.getCodec(), "Codec should be null by default");
        assertNull(queueConfig.getVisibilityTimeout(), "Visibility timeout should be null by default");
        assertFalse(queueConfig.getVisibilityDuplicateProtection(), "Visibility duplicate protection should default to false");
        assertEquals(Duration.ofHours(2), queueConfig.getVisibilityDuplicateProtectionTimeout(),
                "Visibility duplicate protection timeout should default to 2 hours");
        assertNull(queueConfig.getWaitTime(), "Wait time should default to null");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
            "https://sqs.eu-west-1.amazonaws.com/987654321098/my-queue-name",
            "https://sqs.ap-southeast-1.amazonaws.com/111222333444/Queue_With_Underscores",
            "https://sqs.cn-north-1.amazonaws.com.cn/123456789012/ChinaQueue"
    })
    void valid_sqs_url_matches_pattern(final String validUrl) {
        assertTrue(SQS_URL_PATTERN.matcher(validUrl).matches(),
                "Expected valid SQS URL to match pattern: " + validUrl);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
            "http://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
            "https://evil.com/sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
            "https://sqs.us-east-1.amazonaws.com/12345/MyQueue",
            "https://s3.us-east-1.amazonaws.com/mybucket/mykey",
            "ftp://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
    })
    void invalid_sqs_url_does_not_match_pattern(final String invalidUrl) {
        assertFalse(SQS_URL_PATTERN.matcher(invalidUrl).matches(),
                "Expected invalid SQS URL to not match pattern: " + invalidUrl);
    }
}