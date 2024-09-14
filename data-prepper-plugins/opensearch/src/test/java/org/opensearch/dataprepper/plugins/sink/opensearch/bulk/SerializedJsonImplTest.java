/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class SerializedJsonImplTest {
    private int documentSize;
    private byte[] documentBytes;
    private String documentId;
    private String routingField;
    private String pipelineField;

    @BeforeEach
    void setUp() {
        Random random = new Random();
        documentSize = random.nextInt(1_000) + 100;

        documentBytes = new byte[documentSize];
        documentId = RandomStringUtils.randomAlphabetic(10);
        routingField = RandomStringUtils.randomAlphabetic(10);
        pipelineField = RandomStringUtils.randomAlphabetic(10);
    }

    private SerializedJsonImpl createObjectUnderTest() {
        return new SerializedJsonImpl(documentBytes, documentId, routingField, pipelineField);
    }

    @Test
    void getDocumentSize_returns_size_of_the_document_byte_array() {
        assertThat(createObjectUnderTest().getDocumentSize(), equalTo((long) documentSize));
    }

    @Test
    void getSerializedJson_returns_the_document_byte_array_and_fields() {
        assertThat(createObjectUnderTest().getSerializedJson(), sameInstance(documentBytes));
        assertThat(createObjectUnderTest().getDocumentId().get(), equalTo(documentId));
        assertThat(createObjectUnderTest().getRoutingField().get(), equalTo(routingField));
        assertThat(createObjectUnderTest().getPipelineField().get(), equalTo(pipelineField));
    }
}
