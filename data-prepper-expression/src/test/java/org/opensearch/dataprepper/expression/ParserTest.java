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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;

@ExtendWith(MockitoExtension.class)
public class ParserTest {
    private static final Logger LOG = LoggerFactory.getLogger(ParserTest.class);

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
    void bar() {
        withTokenStream(
                DataPrepperExpressionParser.Integer,
                DataPrepperExpressionParser.EQUAL,
                DataPrepperExpressionParser.Float,
                DataPrepperExpressionParser.EOF
        );

        final DataPrepperExpressionParser.ExpressionContext expression = parser.expression();

        assertThat(expression, isA(ParseTree.class));
    }
}
