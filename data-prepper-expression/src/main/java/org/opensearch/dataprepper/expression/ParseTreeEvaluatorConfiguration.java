/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.springframework.context.annotation.Bean;

import javax.inject.Named;

@Named
class ParseTreeEvaluatorConfiguration {
    @Bean
    public ParseTreeWalker parseTreeWalker() {
        return new ParseTreeWalker();
    }
}
