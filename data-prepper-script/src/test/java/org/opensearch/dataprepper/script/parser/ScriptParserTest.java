/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.script.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.opensearch.dataprepper.script.parser.util.ListenerMatcher;
import org.opensearch.dataprepper.script.parser.util.TestListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class ScriptParserTest {
    private static final Logger LOG = LoggerFactory.getLogger(ScriptParserTest.class);

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

        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[[true],'==',[false]]"));
    }

    @Test
    public void testConditionalExpression() {
        parseStatement("false and true");

        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[[false],'and',[true]]"));
    }

    @Test
    public void testMultipleConditionalExpression() {
        parseStatement("false and true or true");

        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[[[false],'and',[true]],'or',[true]]"));
    }

    @Test
    public void testParenthesesExpression() {
        parseStatement("false and (false or true)");

        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[[false],'and',[[[false],'or',[true]]]]"));
    }

    @Test
    public void testInCollectionExpression() {
        parseStatement("2 in [\"1\", 2, 3]");

        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[[2],'in',['[',['1'],',',[2],',',[3],']']]"));
    }

    @Test
    public void testNotInCollectionExpression() {
        parseStatement("true not in [false, true or false]");

        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[[true],'not in',['[',[false],',',[[true],'or',[false]],']']]"));
    }

    @Test
    public void testNestedParenthesisExpression() {
        parseStatement("(1==4)or((2)!=(3==3))");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[[[[1],'==',[4]]],'or',[[[[2]],'!=',[[[3],'==',[3]]]]]]"));
    }

    @Test
    public void testJsonPointerExpression() {
        parseStatement("/a/b/c == true");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[['/a/b/c'],'==',[true]]"));
    }

    @Test
    public void testStringExpression() {
        parseStatement("\"Hello World\" == 42");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[['Hello World'],'==',[42]]"));
    }

    @Test
    public void testEscapeStringExpression() {
        parseStatement("\"Hello \\\"World\\\"\" == 42");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[['Hello \\\"World\\\"'],'==',[42]]"));
    }

    @Test
    public void testEscapeJsonPointer() {
        parseStatement("\"/a b/\\\"c~d\\\"/\\/\"");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("['/a b/\\\"c~d\\\"/\\/']"));
    }

    @Test
    public void testRelationalExpression() {
        parseStatement("1 < 2");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[[1],'<',[2]]"));
    }

    @Test
    public void testMultipleRelationalExpression() {
        parseStatement("1 < 2 or 3 <= 4 or 5 > 6 or 7 >= 8");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[[[[[1],'<',[2]],'or',[[3],'<=',[4]]],'or',[[5],'>',[6]]],'or',[[7],'>=',[8]]]"));
    }

    @Test
    public void testInTermCreatesError() {
        parseStatement("1 in 1");
        assertThat(listener, ListenerMatcher.hasError());
    }

    @Test
    public void testWhiteSpaceInsignificant() {
        int errorCount = 0;
        parseStatement("3 > 1 or (/status_code == 500)");
        errorCount += listener.getErrorNodeList().size();
        final String a = listener.toString();

        parseStatement("3>1or(/status_code==500)");
        errorCount += listener.getErrorNodeList().size();
        final String b = listener.toString();

        assertThat(errorCount, is(0));
        assertThat(a, is(b));
    }

    @Test
    public void testShorthandJsonPointerValidCharacter() {
        parseStatement("/ABCDEFGHIJKLMNOPQRSTUVWXYZ/ambcdefghijklmnopqrstuvwxyz/0123456789/_");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("['/ABCDEFGHIJKLMNOPQRSTUVWXYZ/ambcdefghijklmnopqrstuvwxyz/0123456789/_']"));
    }

    @Test
    public void testStringCharacters() {
        parseStatement("\"Hello, this \\\"is\\\" a 'complex' ~ string with numbers! 0123\"");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("['Hello, this \\\"is\\\" a 'complex' ~ string with numbers! 0123']"));
    }

    @Test
    public void testRegexEqualOperator() {
        parseStatement("\"foo\"=~\"[A-Z]*\"");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[['foo'],'=~',['[A-Z]*']]"));
    }

    @Test
    public void testRegexNotEqualOperator() {
        parseStatement("\"foo\"!~\"[A-Z]*\"");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[['foo'],'!~',['[A-Z]*']]"));
    }
    @Test
    public void testFloatingPointNumber() {
        parseStatement("3.14159");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[3.14159]"));
    }

    @Test
    public void testRegexEqualToLiteralHasErrorNodes() {
        parseStatement("\"foo\"=~3.14");
        assertThat(listener, ListenerMatcher.hasError());
    }

    private Executable assertThatHasParseError(final String statement) {
        return () -> {
            parseStatement(statement);
            assertThat(listener, ListenerMatcher.hasError());
        };
    }

    @Test
    public void testFloatParsingRules() {
        assertAll(
                assertThatHasParseError("0."),
                assertThatHasParseError("00."),
                assertThatHasParseError(".00"),
                assertThatHasParseError(".10"),
                assertThatHasParseError("1.10"),
                () -> {
                    parseStatement("1.0");
                    assertThat(listener, ListenerMatcher.isValid());
                    assertThat(listener.toString(), is("[1.0]"));
                },
                () -> {
                    parseStatement(".0");
                    assertThat(listener, ListenerMatcher.isValid());
                    assertThat(listener.toString(), is("[0.0]"));
                },
                () -> {
                    parseStatement(".1");
                    assertThat(listener, ListenerMatcher.isValid());
                    assertThat(listener.toString(), is("[0.1]"));
                }
        );
    }

    @Test
    public void testStringEscapeCharacters() {
        parseStatement("\"Hello \\\"world\\\", 'this' is a \\\\ string.\"");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("['Hello \\\"world\\\", 'this' is a  string.']"));
    }

    @Test
    public void testMissingParameter() {
        parseStatement("5==");
        assertThat(listener, ListenerMatcher.hasError());
    }

    @Test
    public void testNotOperator() {
        parseStatement("not (5 not in [1]) or not \"in\"");
        assertThat(listener, ListenerMatcher.isValid());
        assertThat(listener.toString(), is("[['not',[[[5],'not in',['[',[1],']']]]],'or',['not',['in']]]"));
    }
}