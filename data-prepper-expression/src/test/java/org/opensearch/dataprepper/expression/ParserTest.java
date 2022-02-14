/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
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
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isParseTree;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

@ExtendWith(MockitoExtension.class)
public class ParserTest {
    private static final Class<? extends ParseTree> Expression = DataPrepperExpressionParser.ExpressionContext.class;
    private static final Class<? extends ParseTree> ConditionalExpression = DataPrepperExpressionParser.ConditionalExpressionContext.class;
    private static final Class<? extends ParseTree> ConditionalOperator = DataPrepperExpressionParser.ConditionalOperatorContext.class;
    private static final Class<? extends ParseTree> EqualityOperatorExpression = DataPrepperExpressionParser.EqualityOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> EqualityOperator = DataPrepperExpressionParser.EqualityOperatorContext.class;
    private static final Class<? extends ParseTree> RegexOperatorExpression = DataPrepperExpressionParser.RegexOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> RegexEqualityOperator = DataPrepperExpressionParser.RegexEqualityOperatorContext.class;
    private static final Class<? extends ParseTree> RelationalOperatorExpression = DataPrepperExpressionParser.RelationalOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> RelationalOperator = DataPrepperExpressionParser.RelationalOperatorContext.class;
    private static final Class<? extends ParseTree> SetOperatorExpression = DataPrepperExpressionParser.SetOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> SetOperator = DataPrepperExpressionParser.SetOperatorContext.class;
    private static final Class<? extends ParseTree> UnaryOperatorExpression = DataPrepperExpressionParser.UnaryOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> ParenthesisExpression = DataPrepperExpressionParser.ParenthesisExpressionContext.class;
    private static final Class<? extends ParseTree> RegexPattern = DataPrepperExpressionParser.RegexPatternContext.class;
    private static final Class<? extends ParseTree> SetInitializer = DataPrepperExpressionParser.SetInitializerContext.class;
    private static final Class<? extends ParseTree> UnaryNotOperatorExpression = DataPrepperExpressionParser.UnaryNotOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> UnaryOperator = DataPrepperExpressionParser.UnaryOperatorContext.class;
    private static final Class<? extends ParseTree> Primary = DataPrepperExpressionParser.PrimaryContext.class;
    private static final Class<? extends ParseTree> JsonPointer = DataPrepperExpressionParser.JsonPointerContext.class;
    private static final Class<? extends ParseTree> VariableIdentifier = DataPrepperExpressionParser.VariableIdentifierContext.class;
    private static final Class<? extends ParseTree> VariableName = DataPrepperExpressionParser.VariableNameContext.class;
    private static final Class<? extends ParseTree> Literal = DataPrepperExpressionParser.LiteralContext.class;

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

        final DataPrepperExpressionParser.ExpressionContext expression = parser.expression();

        assertThat(expression, isParseTree(Expression).withChildrenMatching(
                isParseTree(
                        ConditionalExpression,
                        EqualityOperatorExpression
                ).withChildrenMatching(
                        isParseTree(
                                EqualityOperatorExpression,
                                RegexOperatorExpression,
                                RelationalOperatorExpression,
                                SetOperatorExpression,
                                UnaryOperatorExpression,
                                Primary,
                                Literal
                        ).containingTerminalNode(),
                        isParseTree(EqualityOperator).containingTerminalNode(),
                                isParseTree(
                                        RegexOperatorExpression,
                                        RelationalOperatorExpression,
                                        SetOperatorExpression,
                                        UnaryOperatorExpression,
                                        Primary,
                                        Literal
                                ).containingTerminalNode()
                ),
                isTerminalNode())
        );
    }
}
