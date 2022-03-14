/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class ExpressionEvaluationExceptionTest {

    @Test
    void testExpressionEvaluationException() {
        final ExpressionEvaluationException exception = new ExpressionEvaluationException(UUID.randomUUID().toString(), null);
        assertThat(exception instanceof RuntimeException, is(true));
    }
}