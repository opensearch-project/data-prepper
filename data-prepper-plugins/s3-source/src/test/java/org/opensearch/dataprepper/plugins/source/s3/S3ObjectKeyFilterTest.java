/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanKeyPathOption;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3ObjectKeyFilterTest {

    @Test
    void isKeyMatchingFilters_returns_true_when_filters_is_empty() {
        final S3ObjectKeyFilter filter = new S3ObjectKeyFilter(Collections.emptyMap());
        assertThat(filter.isKeyMatchingFilters("my-bucket", "any/key.json"), equalTo(true));
    }

    @Test
    void isKeyMatchingFilters_returns_true_when_filters_is_null() {
        final S3ObjectKeyFilter filter = new S3ObjectKeyFilter(null);
        assertThat(filter.isKeyMatchingFilters("my-bucket", "any/key.json"), equalTo(true));
    }

    @Test
    void isKeyMatchingFilters_returns_true_when_bucket_not_in_filters() {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(List.of("assets/"));
        final S3ObjectKeyFilter filter = new S3ObjectKeyFilter(Map.of("other-bucket", keyPathOption));

        assertThat(filter.isKeyMatchingFilters("my-bucket", "any/key.json"), equalTo(true));
    }

    @ParameterizedTest
    @CsvSource({
            "assets/image.png, true",
            "logs/app.log, false"
    })
    void isKeyMatchingFilters_with_include_prefix(final String objectKey, final boolean expected) {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(List.of("assets/"));
        when(keyPathOption.getS3ScanExcludeSuffixOptions()).thenReturn(null);
        final S3ObjectKeyFilter filter = new S3ObjectKeyFilter(Map.of("my-bucket", keyPathOption));

        assertThat(filter.isKeyMatchingFilters("my-bucket", objectKey), equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({
            "assets/photo.jpg, false",
            "assets/data.json, true"
    })
    void isKeyMatchingFilters_with_exclude_suffix(final String objectKey, final boolean expected) {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(null);
        when(keyPathOption.getS3ScanExcludeSuffixOptions()).thenReturn(List.of(".jpg", ".xml"));
        final S3ObjectKeyFilter filter = new S3ObjectKeyFilter(Map.of("my-bucket", keyPathOption));

        assertThat(filter.isKeyMatchingFilters("my-bucket", objectKey), equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({
            "assets/data.json, true",
            "assets/photo.jpg, false",
            "logs/app.log, false"
    })
    void isKeyMatchingFilters_with_both_include_prefix_and_exclude_suffix(final String objectKey, final boolean expected) {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(List.of("assets/"));
        when(keyPathOption.getS3ScanExcludeSuffixOptions()).thenReturn(List.of(".jpg"));
        final S3ObjectKeyFilter filter = new S3ObjectKeyFilter(Map.of("my-bucket", keyPathOption));

        assertThat(filter.isKeyMatchingFilters("my-bucket", objectKey), equalTo(expected));
    }

    @Test
    void isKeyMatchingFilters_returns_true_when_key_matches_any_include_prefix() {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(List.of("assets/", "data/"));
        final S3ObjectKeyFilter filter = new S3ObjectKeyFilter(Map.of("my-bucket", keyPathOption));

        assertThat(filter.isKeyMatchingFilters("my-bucket", "data/file.csv"), equalTo(true));
    }
}
