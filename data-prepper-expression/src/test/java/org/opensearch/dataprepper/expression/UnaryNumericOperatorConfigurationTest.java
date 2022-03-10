/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

}