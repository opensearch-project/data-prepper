/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.script.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.script.parser.util.TestListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class ScriptParserTest {
    private static final Logger LOG = LoggerFactory.getLogger(ScriptParserTest.class);
//    "(true == \"test string\") and /a/b not in [1, 2, 3] or ((3.1415 =~ \"foo\") and not /c)"

    private ScriptParser scriptParser;
    private TestListener listener;
    private ParseTreeWalker walker;

    @BeforeEach
    public void beforeEach() {
        scriptParser = new ScriptParser();
        listener = new TestListener();
        walker = new ParseTreeWalker();
    }

    private void parseStatement(final String statement) {
        final ParseTree parseTree = scriptParser.parse(statement);
        walker.walk(listener, parseTree);

        LOG.info("JSON:\n{}", listener.toPrettyString());
    }

    @Test
    public void testEqualityOperator() {
        parseStatement("true==false");

        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[[true],'==',[false]]"));
    }

    @Test
    public void testConditionalExpression() {
        parseStatement("false and true");

        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[[false],'and',[true]]"));
    }

    @Test
    public void testMultipleConditionalExpression() {
        parseStatement("false and true or true");

        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[[[false],'and',[true]],'or',[true]]"));
    }

    @Test
    public void testParenthesesExpression() {
        parseStatement("false and (false or true)");

        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[[false],'and',[[[false],'or',[true]]]]"));
    }

    @Test
    public void testInCollectionExpression() {
        parseStatement("2 in [\"1\", 2, 3]");

        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[[2],'in',['[',['1'],',',[2],',',[3],']']]"));
    }

    @Test
    public void testNotInCollectionExpression() {
        parseStatement("true not in [false, true or false]");

        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[[true],'not in',['[',[false],',',[[true],'or',[false]],']']]"));
    }

    @Test
    public void testNestedParenthesisExpression() {
        parseStatement("(1==4)or((2)!=(3==3))");
        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[[[[1],'==',[4]]],'or',[[[[2]],'!=',[[[3],'==',[3]]]]]]"));
    }

    @Test
    public void testJsonPointerExpression() {
        parseStatement("/a/b/c == true");
        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[['/a/b/c'],'==',[true]]"));
    }

    @Test
    public void testStringExpression() {
        parseStatement("\"Hello World\" == 42");
        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[['Hello World'],'==',[42]]"));
    }

    @Test
    public void testEscapeStringExpression() {
        parseStatement("\"Hello \\\"World\\\"\" == 42");
        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[['Hello \\\"World\\\"'],'==',[42]]"));
    }

    @Test
    public void testEscapeJsonPointer() {
        parseStatement("\"/a b/\\\"c~d\\\"/\\/\"");
        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("['/a b/\\\"c~d\\\"/\\/']"));
    }

    @Test
    public void testRelationalExpression() {
        parseStatement("1 < 2 or 3 <= 4 or 5 > 6 or 7 >= 8");
        assertThat(listener.getErrorNodeList().isEmpty(), is(true));
        assertThat(listener.toString(), is("[[[[[1],'<',[2]],'or',[[3],'<=',[4]]],'or',[[5],'>',[6]]],'or',[[7],'>=',[8]]]"));
    }
}