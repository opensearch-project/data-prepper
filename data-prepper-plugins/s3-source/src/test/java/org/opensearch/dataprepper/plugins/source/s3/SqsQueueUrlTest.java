/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.MalformedURLException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqsQueueUrlTest {
    @Test
    void parse_throws_when_URL_is_null() {
        assertThrows(NullPointerException.class, () -> SqsQueueUrl.parse(null));
    }

    @Test
    void parse_throws_when_URL_is_not_a_URL() {
        assertThrows(MalformedURLException.class, () -> SqsQueueUrl.parse(UUID.randomUUID().toString()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://sqs.us-east-1.amazonaws.com",
            "https://sqs.us-east-1.amazonaws.com/",
            "https://sqs.us-east-1.amazonaws.com/123456789",
            "https://sqs.us-east-1.amazonaws.com/123456789/"
    })
    void parse_throws_when_URL_has_invalid_paths(final String queueUrl) {
        assertThrows(IllegalArgumentException.class, () -> SqsQueueUrl.parse(queueUrl));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "12345678901",
            "1234567890123",
            "A23456789012",
            "12345678901A",
            "12345678901!"
    })
    void parse_throws_when_URL_has_invalid_accountId(final String accountId) {
        final String queueUrl = String.format("https://sqs.us-east-1.amazonaws.com/%s/%s", accountId, UUID.randomUUID());
        assertThrows(IllegalArgumentException.class, () -> SqsQueueUrl.parse(queueUrl));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://sqs.us-east-1.amazonaws.com/%s/%s",
            "https://sqs.us-east-1.amazonaws.com/%s/%s/",
            "https://sqs.us-east-1.amazonaws.com/%s/%s.fifo",
            "https://sqs.us-east-1.amazonaws.com/%s/%s.fifo/",
            "https://sqs.us-west-2.amazonaws.com/%s/%s"
    })
    void getAccountId_returns_accountId_part(final String queueFormatString) throws MalformedURLException {
        final String accountId = randomAccountId();
        final String queueName = UUID.randomUUID().toString();

        final String queueUrl = String.format(queueFormatString, accountId, queueName);

        final SqsQueueUrl objectUnderTest = SqsQueueUrl.parse(queueUrl);

        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getAccountId(), equalTo(accountId));
    }

    private String randomAccountId() {
        return RandomStringUtils.randomNumeric(12);
    }
}