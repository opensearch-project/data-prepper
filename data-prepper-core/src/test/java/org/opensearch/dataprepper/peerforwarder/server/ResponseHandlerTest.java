/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class ResponseHandlerTest {

    private ResponseHandler createObjectUnderTest() {
        return new ResponseHandler();
    }

    @Test
    void test_JsonProcessingException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final JsonProcessingException jsonProcessingException = mock(JsonProcessingException.class);

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(jsonProcessingException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());
    }

    @Test
    void test_UnknownException() throws ExecutionException, InterruptedException {
        final ResponseHandler objectUnderTest = createObjectUnderTest();
        final UnknownException unknownException = new UnknownException("");

        final String testMessage = "test exception message";

        final HttpResponse httpResponse = objectUnderTest.handleException(unknownException, testMessage);
        final AggregatedHttpResponse aggregatedHttpResponse = httpResponse.aggregate().get();

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, aggregatedHttpResponse.status());
        assertEquals(testMessage, aggregatedHttpResponse.contentUtf8());
    }

    static class UnknownException extends Exception {
        public UnknownException(final String message) {
            super(message);
        }
    }

}