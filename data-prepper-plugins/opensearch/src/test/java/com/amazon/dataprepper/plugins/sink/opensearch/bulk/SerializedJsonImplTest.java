/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class SerializedJsonImplTest {
    private int documentSize;
    private byte[] documentBytes;

    @BeforeEach
    void setUp() {
        Random random = new Random();
        documentSize = random.nextInt(1_000) + 100;

        documentBytes = new byte[documentSize];
    }

    private SerializedJsonImpl createObjectUnderTest() {
        return new SerializedJsonImpl(documentBytes);
    }

    @Test
    void getDocumentSize_returns_size_of_the_document_byte_array() {
        assertThat(createObjectUnderTest().getDocumentSize(), equalTo((long) documentSize));
    }

    @Test
    void getSerializedJson_returns_the_document_byte_array() {
        assertThat(createObjectUnderTest().getSerializedJson(), sameInstance(documentBytes));
    }
}