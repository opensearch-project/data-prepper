/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExpressionEvaluatorTest {
    private ExpressionEvaluator expressionEvaluator;
    class TestExpressionEvaluator implements ExpressionEvaluator {
        private final boolean throwsExpressionEvaluationException;
        private final boolean returnNull;
        public TestExpressionEvaluator() {
            throwsExpressionEvaluationException = false;
            returnNull = false;
        }

        public TestExpressionEvaluator(boolean throwsExpressionEvaluationException, boolean returnNull) {
            this.throwsExpressionEvaluationException = throwsExpressionEvaluationException;
            this.returnNull = returnNull;
        }

        public Object evaluate(final String statement, final Event event) {
            if (throwsExpressionEvaluationException) {
                throw new ExpressionEvaluationException("Expression Evaluation Exception", new RuntimeException("runtime exception"));
            } else if (returnNull) {
                return null;
            }
            return event.get(statement, Object.class);
        }

        @Override
        public Boolean isValidExpressionStatement(final String statement) {
            return true;
        }

        @Override
        public Boolean isValidFormatExpression(String format) {
            return true;
        }

        @Override
        public List<String> extractDynamicKeysFromFormatExpression(String format) {
            return Collections.emptyList();
        }

        @Override
        public List<String> extractDynamicExpressionsFromFormatExpression(String format) {
            return Collections.emptyList();
        }
    }

    @Test
    public void testDefaultEvaluateConditional() {
        expressionEvaluator = new TestExpressionEvaluator();
        assertThat(expressionEvaluator.evaluateConditional("/status", event("{\"status\":true}")), equalTo(true));
    }

    @Test
    public void testEvaluateReturningException() {
        expressionEvaluator = new TestExpressionEvaluator();
        assertThat(expressionEvaluator.evaluateConditional("/status", event("{\"nostatus\":true}")), equalTo(false));
    }

    @Test
    public void testThrowExpressionEvaluationException() {
        expressionEvaluator = new TestExpressionEvaluator(true, false);
        assertThat(expressionEvaluator.evaluateConditional("/status", event("{\"nostatus\":true}")), equalTo(false));
    }

    @Test
    public void testExpressionEvaluationReturnsNull() {
        expressionEvaluator = new TestExpressionEvaluator(false, true);
        assertThat(expressionEvaluator.evaluateConditional("/status", event("{\"nostatus\":true}")), equalTo(false));
    }

    @Test
    public void testDefaultEvaluateConditionalThrows() {
        expressionEvaluator = new TestExpressionEvaluator();
        assertThrows(ClassCastException.class, () -> expressionEvaluator.evaluateConditional("/status", event("{\"status\":200}")));
    }

    private static Event event(final String data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }
}


