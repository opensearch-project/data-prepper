/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loggenerator;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CsvLogFakerTest {

    @Test
    public void when_CsvLogFakerVPCFlowLogs_then_hasCorrectFormat() {
        CsvLogFaker objectUnderTest = new CsvLogFaker();

        final String log = objectUnderTest.generateRandomStandardVPCFlowLog();
        final int numberOfSpaces = StringUtils.countMatches(log, " ");
        final int expectedNumberOfSpaces = 12;
        assertThat(numberOfSpaces, equalTo(expectedNumberOfSpaces));
    }
}
