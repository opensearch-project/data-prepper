/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.ownership;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StaticBucketOwnerProviderTest {
    private String accountId;

    @BeforeEach
    void setUp() {
        accountId = UUID.randomUUID().toString();
    }

    private StaticBucketOwnerProvider createObjectUnderTest() {
        return new StaticBucketOwnerProvider(accountId);
    }

    @Test
    void constructor_throws_with_null() {
        accountId = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void getBucketOwner_returns_the_predefined_accountId() {
        final Optional<String> optionalOwner = createObjectUnderTest().getBucketOwner(UUID.randomUUID().toString());

        assertThat(optionalOwner, notNullValue());
        assertThat(optionalOwner.isPresent(), equalTo(true));
        assertThat(optionalOwner.get(), equalTo(accountId));
    }
}