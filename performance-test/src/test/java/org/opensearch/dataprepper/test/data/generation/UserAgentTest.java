/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.data.generation;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

class UserAgentTest {
    @Test
    void userAgent_returns_string() {
        final String userAgent = UserAgent.getInstance().userAgent();

        assertThat(userAgent, notNullValue());
        assertThat(userAgent.length(), greaterThanOrEqualTo(10));

        String expectedRegex = "^[A-Za-z0-9]+/[0-9]+.[0-9]+.[0-9]+ \\([A-Za-z0-9]+; [A-Za-z0-9]+ [0-9]+.[0-9]+.[0-9]+\\)$";

        assertThat(userAgent, matchesPattern(expectedRegex));
    }

    @Test
    void userAgent_returns_unique_value_on_multiple_calls() {
        final UserAgent objectUnderTest = UserAgent.getInstance();
        final String userAgent = objectUnderTest.userAgent();

        assertThat(userAgent, notNullValue());
        assertThat(objectUnderTest.userAgent(), not(equalTo(userAgent)));
        assertThat(objectUnderTest.userAgent(), not(equalTo(userAgent)));
        assertThat(objectUnderTest.userAgent(), not(equalTo(userAgent)));
    }
}