/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.utils;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mockStatic;

class IdentifierShortenerTest {

    @Test
    void shortenIdentifier_when_input_is_shorter_than_max_then_return_original_input() {
        final int maxLength = 10;
        final String testInput = UUID.randomUUID().toString().substring(0, maxLength);

        final String result = IdentifierShortener.shortenIdentifier(testInput, maxLength);

        assertThat(result, equalTo(testInput));
    }

    @Test
    void shortenIdentifier_when_input_is_longer_than_max_then_return_shortened_result() {
        final int maxLength = 5;
        final String testInput = UUID.randomUUID().toString();
        assertThat(testInput.length(), greaterThan(maxLength));

        final String result = IdentifierShortener.shortenIdentifier(testInput, maxLength);

        assertThat(result.length(), lessThanOrEqualTo(maxLength));
    }

    @Test
    void shortenIdentifier_when_NoSuchAlgorithmException_then_return_shortened_result() {
        final int maxLength = 5;
        final String testInput = UUID.randomUUID().toString();
        assertThat(testInput.length(), greaterThan(maxLength));

        try (MockedStatic<MessageDigest> messageDigestMockedStatic = mockStatic(MessageDigest.class)) {
            messageDigestMockedStatic.when(() -> MessageDigest.getInstance("SHA-256"))
                    .thenThrow(new NoSuchAlgorithmException());
            final String result = IdentifierShortener.shortenIdentifier(testInput, maxLength);
            assertThat(result, equalTo(testInput.substring(0, maxLength)));
        }


    }
}