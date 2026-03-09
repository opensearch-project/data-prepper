/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor.s3source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.s3.common.source.S3ObjectReference;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.S3EnrichProcessorConfig;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.configuration.S3EnrichBucketOption;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3ObjectReferenceResolverTest {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String KEY_PATH = "s3_key";
    private static final String NAME_PATTERN = "^(.*)_output\\.jsonl$";

    @Mock
    private S3EnrichProcessorConfig config;

    @Mock
    private S3EnrichBucketOption bucketOption;

    @Mock
    private Event event;

    @BeforeEach
    void setUp() {
        when(config.getEnricherNamePattern()).thenReturn(NAME_PATTERN);
        when(config.getS3EnrichBucketOption()).thenReturn(bucketOption);
        when(config.getEnricherKeyPath()).thenReturn(KEY_PATH);
        when(bucketOption.getName()).thenReturn(BUCKET_NAME);
    }

    private S3ObjectReferenceResolver createObjectUnderTest() {
        return new S3ObjectReferenceResolver(config);
    }

    @Test
    void resolve_returns_S3ObjectReference_with_correct_bucket() {
        when(config.getS3IncludePrefix()).thenReturn(Optional.empty());
        when(event.get(KEY_PATH, String.class)).thenReturn("data/reports/daily_output.jsonl");

        final S3ObjectReference result = createObjectUnderTest().resolve(event);

        assertThat(result, notNullValue());
        assertThat(result.getBucketName(), equalTo(BUCKET_NAME));
    }

    @Test
    void resolve_extracts_base_name_and_appends_jsonl_extension() {
        when(config.getS3IncludePrefix()).thenReturn(Optional.empty());
        when(event.get(KEY_PATH, String.class)).thenReturn("path/to/daily_output.jsonl");

        final S3ObjectReference result = createObjectUnderTest().resolve(event);

        assertThat(result.getKey(), equalTo("daily.jsonl"));
    }

    @Test
    void resolve_prepends_prefix_when_s3IncludePrefix_is_configured() {
        when(config.getS3IncludePrefix()).thenReturn(Optional.of("enrichment/"));
        when(event.get(KEY_PATH, String.class)).thenReturn("data/daily_output.jsonl");

        final S3ObjectReference result = createObjectUnderTest().resolve(event);

        assertThat(result.getKey(), equalTo("enrichment/daily.jsonl"));
    }

    @Test
    void resolve_uses_filename_as_is_when_pattern_does_not_match() {
        when(config.getS3IncludePrefix()).thenReturn(Optional.empty());
        when(event.get(KEY_PATH, String.class)).thenReturn("path/to/some_file.parquet");

        final S3ObjectReference result = createObjectUnderTest().resolve(event);

        assertThat(result.getKey(), equalTo("some_file.parquet"));
    }

    @Test
    void resolve_produces_correct_uri() {
        when(config.getS3IncludePrefix()).thenReturn(Optional.empty());
        when(event.get(KEY_PATH, String.class)).thenReturn("data/report_output.jsonl");

        final S3ObjectReference result = createObjectUnderTest().resolve(event);

        assertThat(result.uri(), equalTo("s3://" + BUCKET_NAME + "/" + result.getKey()));
    }

    @Test
    void resolve_throws_when_s3_key_is_null() {
        when(event.get(KEY_PATH, String.class)).thenReturn(null);

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest().resolve(event));
    }

    @Test
    void resolve_throws_when_s3_key_is_blank() {
        when(event.get(KEY_PATH, String.class)).thenReturn("   ");

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest().resolve(event));
    }

    @Test
    void resolve_throws_when_bucket_name_is_blank() {
        when(bucketOption.getName()).thenReturn("");
        when(event.get(KEY_PATH, String.class)).thenReturn("path/to/file_output.jsonl");

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest().resolve(event));
    }

    @Test
    void resolve_handles_s3_key_at_root_level_without_path_separator() {
        when(config.getS3IncludePrefix()).thenReturn(Optional.empty());
        when(event.get(KEY_PATH, String.class)).thenReturn("myfile_output.jsonl");

        final S3ObjectReference result = createObjectUnderTest().resolve(event);

        assertThat(result.getKey(), equalTo("myfile.jsonl"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "data/subdir/report_output.jsonl",
        "report_output.jsonl",
        "a/b/c/d/report_output.jsonl"
    })
    void resolve_extracts_filename_correctly_from_various_path_depths(final String s3Key) {
        when(config.getS3IncludePrefix()).thenReturn(Optional.empty());
        when(event.get(KEY_PATH, String.class)).thenReturn(s3Key);

        final S3ObjectReference result = createObjectUnderTest().resolve(event);

        assertThat(result, notNullValue());
        assertThat(result.getBucketName(), equalTo(BUCKET_NAME));
    }

    @Test
    void resolve_handles_s3_key_with_trailing_slash_by_stripping_it() {
        when(config.getS3IncludePrefix()).thenReturn(Optional.empty());
        when(event.get(KEY_PATH, String.class)).thenReturn("path/to/dir/");

        // trailing slash is stripped; "dir" is extracted as filename; pattern doesn't match so returned as-is
        final S3ObjectReference result = createObjectUnderTest().resolve(event);

        assertThat(result, notNullValue());
        assertThat(result.getKey(), equalTo("dir"));
    }
}
