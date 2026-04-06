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
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanKeyPathOption;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3ObjectFilteringHelperTest {

    @Test
    void isKeyMatchingFilters_returns_true_when_filters_is_empty() {
        final S3ObjectFilteringHelper helper = new S3ObjectFilteringHelper(Collections.emptyMap());
        assertThat(helper.isKeyMatchingFilters("my-bucket", "any/key.json"), equalTo(true));
    }

    @Test
    void isKeyMatchingFilters_returns_true_when_filters_is_null() {
        final S3ObjectFilteringHelper helper = new S3ObjectFilteringHelper(null);
        assertThat(helper.isKeyMatchingFilters("my-bucket", "any/key.json"), equalTo(true));
    }

    @Test
    void isKeyMatchingFilters_returns_true_when_bucket_not_in_filters() {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(List.of("assets/"));
        final S3ObjectFilteringHelper helper = new S3ObjectFilteringHelper(Map.of("other-bucket", keyPathOption));

        assertThat(helper.isKeyMatchingFilters("my-bucket", "any/key.json"), equalTo(true));
    }

    @Test
    void isKeyMatchingFilters_returns_true_when_key_matches_include_prefix() {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(List.of("assets/"));
        when(keyPathOption.getS3ScanExcludeSuffixOptions()).thenReturn(null);
        final S3ObjectFilteringHelper helper = new S3ObjectFilteringHelper(Map.of("my-bucket", keyPathOption));

        assertThat(helper.isKeyMatchingFilters("my-bucket", "assets/image.png"), equalTo(true));
    }

    @Test
    void isKeyMatchingFilters_returns_false_when_key_does_not_match_include_prefix() {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(List.of("assets/"));
        when(keyPathOption.getS3ScanExcludeSuffixOptions()).thenReturn(null);
        final S3ObjectFilteringHelper helper = new S3ObjectFilteringHelper(Map.of("my-bucket", keyPathOption));

        assertThat(helper.isKeyMatchingFilters("my-bucket", "logs/app.log"), equalTo(false));
    }

    @Test
    void isKeyMatchingFilters_returns_false_when_key_matches_exclude_suffix() {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(null);
        when(keyPathOption.getS3ScanExcludeSuffixOptions()).thenReturn(List.of(".jpg", ".xml"));
        final S3ObjectFilteringHelper helper = new S3ObjectFilteringHelper(Map.of("my-bucket", keyPathOption));

        assertThat(helper.isKeyMatchingFilters("my-bucket", "assets/photo.jpg"), equalTo(false));
    }

    @Test
    void isKeyMatchingFilters_returns_true_when_key_does_not_match_exclude_suffix() {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(null);
        when(keyPathOption.getS3ScanExcludeSuffixOptions()).thenReturn(List.of(".jpg", ".xml"));
        final S3ObjectFilteringHelper helper = new S3ObjectFilteringHelper(Map.of("my-bucket", keyPathOption));

        assertThat(helper.isKeyMatchingFilters("my-bucket", "assets/data.json"), equalTo(true));
    }

    @Test
    void isKeyMatchingFilters_applies_both_include_prefix_and_exclude_suffix() {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(List.of("assets/"));
        when(keyPathOption.getS3ScanExcludeSuffixOptions()).thenReturn(List.of(".jpg"));
        final S3ObjectFilteringHelper helper = new S3ObjectFilteringHelper(Map.of("my-bucket", keyPathOption));

        assertThat(helper.isKeyMatchingFilters("my-bucket", "assets/data.json"), equalTo(true));
        assertThat(helper.isKeyMatchingFilters("my-bucket", "assets/photo.jpg"), equalTo(false));
        assertThat(helper.isKeyMatchingFilters("my-bucket", "logs/app.log"), equalTo(false));
    }

    @Test
    void isKeyMatchingFilters_returns_true_when_key_matches_any_include_prefix() {
        final S3ScanKeyPathOption keyPathOption = mock(S3ScanKeyPathOption.class);
        when(keyPathOption.getS3scanIncludePrefixOptions()).thenReturn(List.of("assets/", "data/"));
        final S3ObjectFilteringHelper helper = new S3ObjectFilteringHelper(Map.of("my-bucket", keyPathOption));

        assertThat(helper.isKeyMatchingFilters("my-bucket", "data/file.csv"), equalTo(true));
    }
}
