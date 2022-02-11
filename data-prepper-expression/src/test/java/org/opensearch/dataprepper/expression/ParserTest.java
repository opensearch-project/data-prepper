/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

@ExtendWith(MockitoExtension.class)
public class ParserTest {

    @Mock
    private Token token;
    @Mock
    private TokenStream tokenStream;
    @InjectMocks
    private DataPrepperExpressionParser parser;

    private int streamPosition = 0;

    @Test
    void foo() {
//        doAnswer(invocation -> {
//            if (streamPosition == 0) {
//                return "5";
//            }
//            else {
//                return "<EOF>";
//            }
//        }).when(token).getText();

        doAnswer(invocation -> {
            if (streamPosition == 0) {
                return DataPrepperExpressionParser.Integer;
            }
            else {
                return DataPrepperExpressionParser.EOF;
            }
        }).when(token).getType();

        doAnswer(invocation -> {
            streamPosition++;
            return null;
        }).when(tokenStream).consume();

        doReturn(token)
                .when(tokenStream).LT(anyInt());

        doAnswer(invocation -> {
            if (streamPosition == 0) {
                return DataPrepperExpressionParser.Integer;
            }
            else {
                return DataPrepperExpressionParser.EOF;
            }
        }).when(tokenStream).LA(anyInt());

        final DataPrepperExpressionParser.ExpressionContext expression = parser.expression();

        assertThat(expression, isA(ParseTree.class));
    }
}
