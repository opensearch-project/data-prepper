/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class AlwaysTrueEvaluatorTest {
    @Test
    void test() {
        final AlwaysTrueEvaluator alwaysTrueEvaluator = new AlwaysTrueEvaluator();
        assertThat(alwaysTrueEvaluator.evaluate(null, null), is(true));
    }
}