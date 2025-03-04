/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class S3BucketAndPrefixTest {
    @ParameterizedTest
    @MethodSource("invalidS3URI")
    void testFromS3UriWithInvalidS3URI(final String invalidS3URI) {
        assertThrows(IllegalArgumentException.class, () -> S3BucketAndPrefix.fromS3Uri(invalidS3URI));
    }

    @Test
    void testFromS3URIWithBucketNameOnly() {
        final String testBucketName = "test-bucket";
        final S3BucketAndPrefix s3BucketAndPrefix = S3BucketAndPrefix.fromS3Uri(
                String.format("s3://%s", testBucketName));
        assertThat(s3BucketAndPrefix.getBucketName(), equalTo(testBucketName));
        assertThat(s3BucketAndPrefix.getPrefix(), equalTo(""));
    }

    @Test
    void testFromS3URIWithBucketNameAndPrefix() {
        final String testBucketName = "test-bucket";
        final String testPrefix = "test-prefix";
        final S3BucketAndPrefix s3BucketAndPrefix = S3BucketAndPrefix.fromS3Uri(
                String.format("s3://%s/%s", testBucketName, testPrefix));
        assertThat(s3BucketAndPrefix.getBucketName(), equalTo(testBucketName));
        assertThat(s3BucketAndPrefix.getPrefix(), equalTo(testPrefix));
    }

    private static Stream<Arguments> invalidS3URI() {
        return Stream.of(null, arguments("invalid-uri"));
    }
}