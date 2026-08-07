/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class ExpressionParsingExceptionTest {

    @Test
    void testExpressionParsingException() {
        final ExpressionParsingException exception = new ExpressionParsingException(UUID.randomUUID().toString(), null);
        assertThat(exception instanceof RuntimeException, is(true));
    }
}

