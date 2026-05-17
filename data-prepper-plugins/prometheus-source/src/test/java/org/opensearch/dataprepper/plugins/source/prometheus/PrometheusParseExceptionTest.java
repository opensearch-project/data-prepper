/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PrometheusParseExceptionTest {

    @Test
    void testExceptionWithMessage() {
        final PrometheusParseException exception = new PrometheusParseException("test message");

        assertThat(exception.getMessage(), equalTo("test message"));
    }

    @Test
    void testExceptionWithMessageAndCause() {
        final RuntimeException cause = new RuntimeException("root cause");
        final PrometheusParseException exception = new PrometheusParseException("test message", cause);

        assertThat(exception.getMessage(), equalTo("test message"));
        assertThat(exception.getCause(), instanceOf(RuntimeException.class));
        assertNotNull(exception.getCause());
    }
}