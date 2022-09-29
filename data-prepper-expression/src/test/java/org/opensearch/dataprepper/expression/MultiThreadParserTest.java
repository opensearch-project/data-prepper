/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MultiThreadParserTest {

    @Mock
    private ApplicationContext applicationContext;
    private String expression;

    @BeforeEach
    void setUp() {
        expression = UUID.randomUUID().toString();
    }

    private MultiThreadParser createObjectUnderTest() {
        return new MultiThreadParser(applicationContext);
    }

    @Test
    void parse_returns_parse_result_from_ApplicationContext_Parser() {
        final Parser<ParseTree> parser = mock(Parser.class);
        when(applicationContext.getBean(ParseTreeParser.SINGLE_THREAD_PARSER_NAME, Parser.class))
                .thenReturn(parser);
        final ParseTree parseTree = mock(ParseTree.class);
        when(parser.parse(expression))
                .thenReturn(parseTree);

        assertThat(createObjectUnderTest().parse(expression), equalTo(parseTree));
    }

    @Test
    void parse_called_multiple_times_only_calls_getBean_once() {
        final Parser<ParseTree> parser = mock(Parser.class);
        when(applicationContext.getBean(ParseTreeParser.SINGLE_THREAD_PARSER_NAME, Parser.class))
                .thenReturn(parser);
        final ParseTree parseTree = mock(ParseTree.class);
        when(parser.parse(expression))
                .thenReturn(parseTree);

        final MultiThreadParser objectUnderTest = createObjectUnderTest();
        for(int i = 0; i < 10; i++)
            objectUnderTest.parse(expression);

        verify(applicationContext).getBean(anyString(), any(Class.class));
    }

    @Test
    void parse_on_multiple_threads_returns_different_ParseTree() throws ExecutionException, InterruptedException, TimeoutException {
        final Parser<ParseTree> firstParser = mock(Parser.class);
        final Parser<ParseTree> secondParser = mock(Parser.class);
        when(applicationContext.getBean(ParseTreeParser.SINGLE_THREAD_PARSER_NAME, Parser.class))
                .thenReturn(firstParser)
                .thenReturn(secondParser);
        final ParseTree firstParseTree = mock(ParseTree.class);
        final ParseTree secondParseTree = mock(ParseTree.class);
        when(firstParser.parse(expression)).thenReturn(firstParseTree);
        when(secondParser.parse(expression)).thenReturn(secondParseTree);

        final MultiThreadParser objectUnderTest = createObjectUnderTest();
        final ParseTree mainThreadParseTree = objectUnderTest.parse(expression);

        final Future<ParseTree> futureParseTree = Executors.newFixedThreadPool(1).submit(() -> objectUnderTest.parse(expression));

        final ParseTree otherThreadParseTree = futureParseTree.get(1, TimeUnit.SECONDS);

        assertThat(mainThreadParseTree, not(sameInstance(otherThreadParseTree)));

        assertThat(mainThreadParseTree, equalTo(firstParseTree));
        assertThat(otherThreadParseTree, equalTo(otherThreadParseTree));
    }
}