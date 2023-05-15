/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.expression.util.GrammarTest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class ParserTest extends GrammarTest {

    private void assertThatIsValid(final String expression) {
        parseExpression(expression);
        assertThat(
                "\"" + expression + "\" should not have parsing errors",
                errorListener.isErrorFound(),
                is(false)
        );
    }

    private void assertThatHasParseError(final String expression) {
        parseExpression(expression);
        assertThat(
                "\"" + expression + "\" should have parsing errors",
                errorListener.isErrorFound(),
                is(true)
        );
    }

    @Test
    void testGivenOperandIsNotStringWhenEvaluateThenErrorPresent() {
        assertThatHasParseError("(true) =~ \"Hello?\"");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{true or false}",
            "{(5)}",
            "{5 == 5}",
            "{/node =~ \"[A-Z]*\"}",
    })
    void testInvalidSetItem(final String expression) {
        assertThatHasParseError(expression);
    }

    @Test
    void testInvalidSetOperator() {
        assertThatHasParseError("1 in 1");
    }

    @Test
    void testInvalidRegexOperand() {
        assertThatHasParseError("\"foo\"=~3.14");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "0.",
            "00.",
            ".00",
            ".10",
    })
    void testInvalidFloatParsingRules(final String expression) {
        assertThatHasParseError(expression);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "5==",
            "==",
            "not",
    })
    void testInvalidNumberOfOperands(final String expression) {
        assertThatHasParseError(expression);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/status in {200}",
            "/a==(/b==200)",
            "/a in {200}",
            "/a not in {400}",
            "/a<300 and /b>200",
    })
    void testValidOperatorsRequireSpace(final String expression) {
        assertThatIsValid(expression);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/a in ({200})",
            "/a in 5",
            "/a in 3.14",
            "/a in false",
            "\"Hello\" not in ({200})",
            "\"Hello\" not in 5",
            "\"Hello\" not in 3.14",
            "\"Hello\" not in false",
    })
    void testInvalidSetOperatorArgs(final String expression) {
        assertThatHasParseError(expression);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/status in{200}",
            "/status in({200})",
            "/a in{200, 202}",
            "/a not in{400}",
            "/b<300and/b>200",
            "/a in {200,}",
    })
    void testInvalidOperatorsRequireSpace(final String expression) {
        assertThatHasParseError(expression);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/status < 300",
            "/status>=300",
            "/msg =~ \"^\\\\w*\\$\"",
            "/msg=~\"[A-Za-z]\"",
            "/status == 200",
            "/status_code==200",
            "/a in {200, 202}",
            "/a in {200,202}",
            "/a in {200 , 202}",
    })
    void testValidOptionalSpaceOperators(final String expression) {
        assertThatIsValid(expression);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not false",
            "not not false",
            "not /status_code",
            "not (true)",
            "-5",
            "-3.14",
            "-(5)"
    })
    void testValidUnaryOperators(final String expression) {
        assertThatIsValid(expression);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "falkse and true",
            "true an true",
            "2 is in {2}",
            "2 in {2,,}"
    })
    void testTypo(final String expression) {
        assertThatHasParseError(expression);
    }
}
