/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;

class UnaryNumericOperatorConfigurationTest {
    private UnaryNumericOperatorConfiguration configuration;

    @BeforeEach
    void beforeEach() {
        configuration = new UnaryNumericOperatorConfiguration();
    }

    @Test
    void testSubtractUnaryNumericOperator() {
        final UnaryNumericOperator subtractUnaryNumericOperator = configuration.subtractUnaryNumericOperator();

        assertThat(subtractUnaryNumericOperator, isA(UnaryNumericOperator.class));
    }

    @ParameterizedTest
    @ValueSource(ints = {
            -5,
            0,
            10,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE
    })
    void testIntegerSubtractUnaryNumericOperator(final Integer arg) {
        final UnaryNumericOperator subtractUnaryNumericOperator = configuration.subtractUnaryNumericOperator();
        final Number result = subtractUnaryNumericOperator.evaluate(arg);
        assertThat(result, is(-arg));
    }

    @ParameterizedTest
    @ValueSource(floats = {
            -42.95f,
            0.0f,
            9999.123f,
            Float.MIN_VALUE,
            Float.MAX_VALUE,
            Float.MIN_NORMAL
    })
    void testFloatSubtractUnaryNumericOperator(final Float arg) {
        final UnaryNumericOperator subtractUnaryNumericOperator = configuration.subtractUnaryNumericOperator();
        final Number result = subtractUnaryNumericOperator.evaluate(arg);
        assertThat(result, is(-arg));
    }
}