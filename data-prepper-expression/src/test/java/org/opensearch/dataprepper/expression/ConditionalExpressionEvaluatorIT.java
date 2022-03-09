/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.inject.Named;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

class ConditionalExpressionEvaluatorIT {

    private AnnotationConfigApplicationContext applicationContext;

    @BeforeEach
    void beforeEach() {
        applicationContext = new AnnotationConfigApplicationContext();
        applicationContext.scan("org.opensearch.dataprepper.expression");
        applicationContext.refresh();
    }

    @Test
    void testConditionalExpressionEvaluatorBeanAvailable() {
        final ConditionalExpressionEvaluator evaluator = applicationContext.getBean(ConditionalExpressionEvaluator.class);
        assertThat(evaluator, isA(ConditionalExpressionEvaluator.class));
    }

    @Test
    void testConditionalExpressionEvaluatorBeanNotSingleton() {
        final ConditionalExpressionEvaluator instanceA = applicationContext.getBean(ConditionalExpressionEvaluator.class);
        final ConditionalExpressionEvaluator instanceB = applicationContext.getBean(ConditionalExpressionEvaluator.class);
        assertThat(instanceA, not(is(instanceB)));
    }

    @Named
    private static class AlwaysTrueEvaluator implements Evaluator<ParseTree, Event> {

        @Override
        public Object evaluate(final ParseTree parseTree, final Event event) throws ClassCastException {
            return true;
        }
    }

}