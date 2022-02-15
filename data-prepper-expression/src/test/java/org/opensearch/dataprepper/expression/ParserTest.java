/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.expression.util.MockTokenStreamHelper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isParseTree;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

@ExtendWith(MockitoExtension.class)
public class ParserTest {
    private static final Class<? extends ParseTree> EXPRESSION = DataPrepperExpressionParser.ExpressionContext.class;
    private static final Class<? extends ParseTree> CONDITIONAL_EXPRESSION = DataPrepperExpressionParser.ConditionalExpressionContext.class;
    private static final Class<? extends ParseTree> EQUALITY_OPERATOR_EXPRESSION = DataPrepperExpressionParser.EqualityOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> EQUALITY_OPERATOR = DataPrepperExpressionParser.EqualityOperatorContext.class;
    private static final Class<? extends ParseTree> REGEX_OPERATOR_EXPRESSION = DataPrepperExpressionParser.RegexOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> RELATIONAL_OPERATOR_EXPRESSION = DataPrepperExpressionParser.RelationalOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> SET_OPERATOR_EXPRESSION = DataPrepperExpressionParser.SetOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> UNARY_OPERATOR_EXPRESSION = DataPrepperExpressionParser.UnaryOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> PRIMARY = DataPrepperExpressionParser.PrimaryContext.class;
    private static final Class<? extends ParseTree> LITERAL = DataPrepperExpressionParser.LiteralContext.class;

    @Mock
    private TokenStream tokenStream;
    @InjectMocks
    private DataPrepperExpressionParser parser;

    private void withTokenStream(final Integer ... types) {
        final MockTokenStreamHelper helper = new MockTokenStreamHelper(types);

        doAnswer(helper::consume).when(tokenStream).consume();
        doAnswer(helper::LT).when(tokenStream).LT(anyInt());
        doAnswer(helper::LA).when(tokenStream).LA(anyInt());

        parser.setInputStream(tokenStream);
    }

    @Test
    void testEqualityExpression() {
        withTokenStream(
                DataPrepperExpressionParser.Integer,
                DataPrepperExpressionParser.EQUAL,
                DataPrepperExpressionParser.Float,
                DataPrepperExpressionParser.EOF
        );

        final ParseTree expression = parser.expression();

        final DiagnosingMatcher<ParseTree> equals = isParseTree(EQUALITY_OPERATOR).containingTerminalNode();
        final DiagnosingMatcher<ParseTree> endsWithInteger = isParseTree(
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION,
                SET_OPERATOR_EXPRESSION,
                UNARY_OPERATOR_EXPRESSION,
                PRIMARY,
                LITERAL
        ).containingTerminalNode();
        final DiagnosingMatcher<ParseTree> leftHandSide = hasContext(EQUALITY_OPERATOR_EXPRESSION, endsWithInteger);
        final DiagnosingMatcher<ParseTree> equalityExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(leftHandSide, equals, endsWithInteger);

        assertThat(expression, isParseTree(EXPRESSION).withChildrenMatching(equalityExpression, isTerminalNode())
        );
    }
}
