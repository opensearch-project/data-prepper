/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.NoSuchElementException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StsArnRoleTest {
    @Test
    void parse_throws_when_sts_arn_is_null() {
        assertThrows(NullPointerException.class, () -> StsArnRole.parse(null));
    }

    @Test
    void parse_throws_when_sts_arn_is_not_a_arn() {
        assertThrows(IllegalArgumentException.class, () -> StsArnRole.parse(UUID.randomUUID().toString()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "arn:aws:iam::250994255:role/dtest",
            "arn:aws:iam::1:role/dtest",
            "arn:aws:iam::1234567:role/dtest"
    })
    void parse_throws_when_arn_has_invalid_paths(final String arn) {
        assertThrows(IllegalArgumentException.class, () -> StsArnRole.parse(arn));
    }
    @ParameterizedTest
    @ValueSource(strings = {
            "arn:aws:iam:::role/dtest"
    })
    void parse_throws_when_arn_has_empty_accountId_path(final String arn) {
        assertThrows(NoSuchElementException.class, () -> StsArnRole.parse(arn));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "12345678901",
            "1234567890123",
            "A23456789012",
            "12345678901A",
            "12345678901!"
    })
    void parse_throws_when_arn_has_invalid_accountId(final String accountId) {
        final String arnString = String.format("arn:aws:iam::%s:role/%s", accountId, UUID.randomUUID());
        assertThrows(IllegalArgumentException.class, () -> StsArnRole.parse(arnString));
    }

    @Test
    void getAccountId_returns_accountId_part() {
        final String accountId = randomAccountId();
        final String stsArnString = String.format("arn:aws:iam::%s:role/dtest", accountId);

        final StsArnRole objectUnderTest = StsArnRole.parse(stsArnString);

        assertThat(objectUnderTest, notNullValue());
        assertThat(objectUnderTest.getAccountId(), equalTo(accountId));
    }

    private String randomAccountId() {
        return RandomStringUtils.randomNumeric(12);
    }

}