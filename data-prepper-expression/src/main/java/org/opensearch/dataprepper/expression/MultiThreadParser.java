/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.tree.ParseTree;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Objects;

import static org.opensearch.dataprepper.expression.ParseTreeParser.SINGLE_THREAD_PARSER_NAME;

@Named
@Primary
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
class MultiThreadParser implements Parser<ParseTree> {
    private final ApplicationContext applicationContext;
    private final ThreadLocal<Parser<ParseTree>> threadLocalParser;

    @Inject
    MultiThreadParser(final ApplicationContext applicationContext) {
        this.applicationContext = Objects.requireNonNull(applicationContext);
        threadLocalParser = ThreadLocal.withInitial(() -> (Parser<ParseTree>) applicationContext.getBean(SINGLE_THREAD_PARSER_NAME, Parser.class));
    }

    @Override
    public ParseTree parse(final String expression) throws ParseTreeCompositeException {
        return threadLocalParser.get().parse(expression);
    }
}
