/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;

class ParseTreeParserConfigurationTest {

    @Test
    void testDataPrepperExpressionParser() {
        final ParseTreeParserConfiguration parseTreeParserConfiguration = new ParseTreeParserConfiguration();

        final DataPrepperExpressionParser dataPrepperExpressionParser = parseTreeParserConfiguration.dataPrepperExpressionParser();

        assertThat(dataPrepperExpressionParser, isA(DataPrepperExpressionParser.class));
    }
}