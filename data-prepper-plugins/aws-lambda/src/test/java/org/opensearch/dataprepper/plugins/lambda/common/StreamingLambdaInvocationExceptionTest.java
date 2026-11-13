/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.common;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class StreamingLambdaInvocationExceptionTest {

    @Test
    void testExceptionCarriesErrorCodeAndDetails() {
        final StreamingLambdaInvocationException exception =
                new StreamingLambdaInvocationException("Sandbox.Timedout", "Task timed out after 3.00 seconds");

        assertThat(exception.getErrorCode(), equalTo("Sandbox.Timedout"));
        assertThat(exception.getMessage(), equalTo("Sandbox.Timedout: Task timed out after 3.00 seconds"));
    }

    @Test
    void testExceptionWithoutDetails_MessageIsErrorCode() {
        final StreamingLambdaInvocationException exception =
                new StreamingLambdaInvocationException("Unhandled", null);

        assertThat(exception.getMessage(), equalTo("Unhandled"));
    }
}
