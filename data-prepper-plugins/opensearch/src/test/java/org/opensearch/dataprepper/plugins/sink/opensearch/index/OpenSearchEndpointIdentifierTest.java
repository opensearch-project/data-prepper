/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OpenSearchEndpointIdentifierTest {

    @Nested
    class ExtractCollectionIdTests {

        @Test
        void test_extractCollectionId_returnsFirstSegmentOfHostname() {
            final String collectionId = "fh1ves1t1dbd0n9i51nh";
            final List<String> hosts = List.of("https://" + collectionId + ".us-west-2.aoss.amazonaws.com");

            final String result = OpenSearchEndpointIdentifier.extractCollectionId(hosts);

            assertThat(result, equalTo(collectionId));
        }

        @Test
        void test_extractCollectionId_randomId_returnsFirstSegment() {
            final String collectionId = UUID.randomUUID().toString().replace("-", "").substring(0, 20);
            final List<String> hosts = List.of("https://" + collectionId + ".eu-west-1.aoss.amazonaws.com");

            final String result = OpenSearchEndpointIdentifier.extractCollectionId(hosts);

            assertThat(result, equalTo(collectionId));
        }

        @Test
        void test_extractCollectionId_multipleHosts_usesFirstHost() {
            final String collectionId = "abc123def456ghi789jk";
            final List<String> hosts = List.of(
                    "https://" + collectionId + ".us-east-1.aoss.amazonaws.com",
                    "https://other12345678901234.us-east-1.aoss.amazonaws.com"
            );

            final String result = OpenSearchEndpointIdentifier.extractCollectionId(hosts);

            assertThat(result, equalTo(collectionId));
        }

        @Test
        void test_extractCollectionId_nullHosts_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class,
                    () -> OpenSearchEndpointIdentifier.extractCollectionId(null));
        }

        @Test
        void test_extractCollectionId_emptyHosts_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class,
                    () -> OpenSearchEndpointIdentifier.extractCollectionId(Collections.emptyList()));
        }

        @Test
        void test_extractCollectionId_unparsableHost_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class,
                    () -> OpenSearchEndpointIdentifier.extractCollectionId(List.of("not-a-valid-uri")));
        }
    }

    @Nested
    class ExtractDomainNameTests {

        @ParameterizedTest
        @CsvSource({
                "search-mydomain-abc123def.us-west-2.es.amazonaws.com, mydomain",
                "search-my-domain-abc123def.us-west-2.es.amazonaws.com, my-domain",
                "search-my-cool-domain-abc123def.us-west-2.es.amazonaws.com, my-cool-domain",
                "vpc-mydomain-abc123def.us-west-2.es.amazonaws.com, mydomain",
                "vpc-my-domain-abc123def.us-west-2.es.amazonaws.com, my-domain",
                "vpc-multi-hyphen-domain-name-xyz789.eu-west-1.es.amazonaws.com, multi-hyphen-domain-name"
        })
        void test_extractDomainName_validHost_returnsDomainName(final String hostname, final String expectedDomain) {
            final List<String> hosts = List.of("https://" + hostname);

            final String result = OpenSearchEndpointIdentifier.extractDomainName(hosts);

            assertThat(result, equalTo(expectedDomain));
        }

        @Test
        void test_extractDomainName_multipleHosts_usesFirstHost() {
            final List<String> hosts = List.of(
                    "https://search-firstdomain-abc123.us-west-2.es.amazonaws.com",
                    "https://search-seconddomain-def456.us-west-2.es.amazonaws.com"
            );

            final String result = OpenSearchEndpointIdentifier.extractDomainName(hosts);

            assertThat(result, equalTo("firstdomain"));
        }

        @Test
        void test_extractDomainName_nullHosts_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class,
                    () -> OpenSearchEndpointIdentifier.extractDomainName(null));
        }

        @Test
        void test_extractDomainName_emptyHosts_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class,
                    () -> OpenSearchEndpointIdentifier.extractDomainName(Collections.emptyList()));
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "https://search-x.us-west-2.es.amazonaws.com",
                "https://vpc-x.us-west-2.es.amazonaws.com"
        })
        void test_extractDomainName_noHyphenAfterPrefixStrip_throwsIllegalArgumentException(final String host) {
            assertThrows(IllegalArgumentException.class,
                    () -> OpenSearchEndpointIdentifier.extractDomainName(List.of(host)));
        }

        @Test
        void test_extractDomainName_noPrefixNoHyphen_throwsIllegalArgumentException() {
            assertThrows(IllegalArgumentException.class,
                    () -> OpenSearchEndpointIdentifier.extractDomainName(
                            List.of("https://singlesegment.us-west-2.es.amazonaws.com")));
        }
    }

    @Nested
    class GetHostnameTests {

        @Test
        void test_getHostname_validUrl_returnsHostname() {
            final String expected = "myhost.us-west-2.aoss.amazonaws.com";
            final List<String> hosts = List.of("https://" + expected);

            final String result = OpenSearchEndpointIdentifier.getHostname(hosts);

            assertThat(result, equalTo(expected));
        }

        @Test
        void test_getHostname_urlWithPort_returnsHostnameWithoutPort() {
            final String expected = "myhost.us-west-2.es.amazonaws.com";
            final List<String> hosts = List.of("https://" + expected + ":443");

            final String result = OpenSearchEndpointIdentifier.getHostname(hosts);

            assertThat(result, equalTo(expected));
        }

        @Test
        void test_getHostname_urlWithPath_returnsHostname() {
            final String expected = "myhost.us-west-2.es.amazonaws.com";
            final List<String> hosts = List.of("https://" + expected + "/some/path");

            final String result = OpenSearchEndpointIdentifier.getHostname(hosts);

            assertThat(result, equalTo(expected));
        }

        @Test
        void test_getHostname_nullHosts_throwsIllegalArgumentException() {
            final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> OpenSearchEndpointIdentifier.getHostname(null));

            assertThat(exception.getMessage(), equalTo("Hosts list is empty, cannot extract endpoint identifier"));
        }

        @Test
        void test_getHostname_emptyHosts_throwsIllegalArgumentException() {
            final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> OpenSearchEndpointIdentifier.getHostname(Collections.emptyList()));

            assertThat(exception.getMessage(), equalTo("Hosts list is empty, cannot extract endpoint identifier"));
        }

        @Test
        void test_getHostname_unparsableHostname_throwsIllegalArgumentException() {
            final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> OpenSearchEndpointIdentifier.getHostname(List.of("not-a-valid-uri")));

            assertThat(exception.getMessage(), equalTo("Unable to parse hostname from: not-a-valid-uri"));
        }
    }
}
