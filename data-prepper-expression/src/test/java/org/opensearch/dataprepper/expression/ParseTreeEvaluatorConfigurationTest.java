/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;

class ParseTreeEvaluatorConfigurationTest {

    @Test
    void parseTreeWalker() {
        final ParseTreeEvaluatorConfiguration configuration = new ParseTreeEvaluatorConfiguration();
        assertThat(configuration.parseTreeWalker(), isA(ParseTreeWalker.class));
    }
}
