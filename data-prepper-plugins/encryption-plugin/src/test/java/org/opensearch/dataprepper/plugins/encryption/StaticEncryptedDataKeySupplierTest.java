/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class StaticEncryptedDataKeySupplierTest {
    private static final String TEST_ENCRYPTED_DATA_KEY = UUID.randomUUID().toString();
    private StaticEncryptedDataKeySupplier objectUnderTest;

    @Test
    void testRetrieveValue() {
        objectUnderTest = new StaticEncryptedDataKeySupplier(TEST_ENCRYPTED_DATA_KEY);
        assertThat(objectUnderTest.retrieveValue(), equalTo(TEST_ENCRYPTED_DATA_KEY));
    }

    @Test
    void testRefresh() {
        objectUnderTest = new StaticEncryptedDataKeySupplier(TEST_ENCRYPTED_DATA_KEY);
        objectUnderTest.refresh();
        assertThat(objectUnderTest.retrieveValue(), equalTo(TEST_ENCRYPTED_DATA_KEY));
    }
}