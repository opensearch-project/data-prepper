/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class DeclaredOpenSearchVersionTest {
    @ParameterizedTest
    @CsvSource({
            "opensearch:1.2.4,OPENSEARCH,1.2.4",
            "opensearch:1.3.9,OPENSEARCH,1.3.9",
            "opensearch:2.2.1,OPENSEARCH,2.2.1",
            "opensearch:2.6.0,OPENSEARCH,2.6.0",
            "opendistro:1.3.0,OPENDISTRO,1.3.0",
            "opendistro:1.13.3,OPENDISTRO,1.13.3"
    })
    void parse_should_return_expected(final String versionString, final DeclaredOpenSearchVersion.Distribution expectedDistribution, final String expectedVersion) {
        final DeclaredOpenSearchVersion version = DeclaredOpenSearchVersion.parse(versionString);

        assertThat(version, notNullValue());
        assertThat(version.getDistribution(), equalTo(expectedDistribution));
        assertThat(version.getVersion(), equalTo(expectedVersion));
    }

    @Test
    void parse_with_null_should_return_minimum_version() {
        final DeclaredOpenSearchVersion version = DeclaredOpenSearchVersion.parse(null);

        assertThat(version, notNullValue());
        assertThat(version.getDistribution(), equalTo(DeclaredOpenSearchVersion.Distribution.OPENSEARCH));
        assertThat(version.getVersion(), equalTo("1.0.0"));
    }

    @ParameterizedTest
    @CsvSource({
            "opensearch:1.2.4,opensearch:1.2.4,0",
            "opendistro:1.13.3,opendistro:1.13.3,0",
            "opendistro:0.10.0,opendistro:1.0.0,-1",
            "opensearch:1.2.4,opensearch:1.2.3,1",
            "opensearch:1.2.3,opensearch:1.2.4,-1",
            "opensearch:2.6.0,opensearch:1.2.3,1",
            "opensearch:1.2.3,opensearch:2.6.0,-1",
            "opensearch:1.3.9,opendistro:1.13.3,1",
            "opendistro:1.13.3,opensearch:1.3.9,-1",
    })
    void compareTo_returns_correct_value(final String versionStringToTest, final String otherVersionString, final int expectedCompareTo) {
        final DeclaredOpenSearchVersion objectUnderTest = DeclaredOpenSearchVersion.parse(versionStringToTest);
        final DeclaredOpenSearchVersion otherVersion = DeclaredOpenSearchVersion.parse(otherVersionString);

        assertThat(objectUnderTest.compareTo(otherVersion), equalTo(expectedCompareTo));
    }
}