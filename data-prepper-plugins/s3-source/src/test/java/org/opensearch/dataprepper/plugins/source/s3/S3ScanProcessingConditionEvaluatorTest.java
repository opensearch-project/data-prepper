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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanProcessingCondition;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3ScanProcessingConditionEvaluatorTest {

    private static final String BUCKET = "test-bucket";
    private static final String OBJECT_KEY = "output/job-123/result.jsonl.out";
    private static final String MANIFEST_FILE_NAME = "manifest.json.out";
    private static final String MANIFEST_KEY = "output/job-123/" + MANIFEST_FILE_NAME;
    private static final String WHEN_EXPRESSION = "/processedRecordCount == /totalRecordCount";

    @Mock
    private S3Client s3Client;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private S3ScanProcessingConditionEvaluator objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new S3ScanProcessingConditionEvaluator(s3Client, expressionEvaluator);
    }

    // -------------------------------------------------------------------------
    // allConditionsMet — null / empty guards
    // -------------------------------------------------------------------------

    @Test
    void allConditionsMet_when_conditions_is_null_then_returns_true() {
        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, null), is(true));
        verify(s3Client, never()).getObject(any(GetObjectRequest.class));
    }

    @Test
    void allConditionsMet_when_conditions_is_empty_then_returns_true() {
        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, Collections.emptyList()), is(true));
        verify(s3Client, never()).getObject(any(GetObjectRequest.class));
    }

    // -------------------------------------------------------------------------
    // allConditionsMet — include_prefix filtering
    // -------------------------------------------------------------------------

    @Test
    void allConditionsMet_when_object_key_does_not_match_include_prefix_then_condition_is_skipped() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("different-prefix/"));

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(condition)), is(true));
        verify(s3Client, never()).getObject(any(GetObjectRequest.class));
    }

    @Test
    void allConditionsMet_when_include_prefix_is_empty_list_condition_applies_to_all_objects() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, Collections.emptyList());
        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":100,\"processedRecordCount\":100}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(true);

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(condition)), is(true));
    }

    @Test
    void allConditionsMet_when_include_prefix_is_null_condition_applies_to_all_objects() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, null);
        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":100,\"processedRecordCount\":100}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(true);

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(condition)), is(true));
    }

    @Test
    void allConditionsMet_when_object_key_matches_one_of_multiple_prefixes_then_condition_applies() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("no-match/", "output/"));
        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":50,\"processedRecordCount\":50}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(true);

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(condition)), is(true));
    }

    // -------------------------------------------------------------------------
    // allConditionsMet — expression evaluation
    // -------------------------------------------------------------------------

    @Test
    void allConditionsMet_when_expression_evaluates_to_true_then_returns_true() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));
        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":200,\"processedRecordCount\":200}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(true);

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(condition)), is(true));
    }

    @Test
    void allConditionsMet_when_expression_evaluates_to_false_then_returns_false() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));
        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":200,\"processedRecordCount\":100}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(false);

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(condition)), is(false));
    }

    @Test
    void allConditionsMet_passes_manifest_json_fields_to_expression_evaluator() {
        final String manifestJson = "{\"totalRecordCount\":50000,\"processedRecordCount\":8088}";
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, null);
        stubManifest(MANIFEST_KEY, manifestJson);

        final ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), eventCaptor.capture())).thenReturn(true);

        objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(condition));

        final Event capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.get("totalRecordCount", Integer.class), equalTo(50000));
        assertThat(capturedEvent.get("processedRecordCount", Integer.class), equalTo(8088));
    }

    // -------------------------------------------------------------------------
    // allConditionsMet — manifest key resolution
    // -------------------------------------------------------------------------

    @Test
    void allConditionsMet_manifest_key_uses_same_directory_as_object_key() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, null);
        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":1,\"processedRecordCount\":1}");
        when(expressionEvaluator.evaluateConditional(any(), any())).thenReturn(true);

        objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(condition));

        final ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(requestCaptor.capture());
        assertThat(requestCaptor.getValue().key(), equalTo(MANIFEST_KEY));
        assertThat(requestCaptor.getValue().bucket(), equalTo(BUCKET));
    }

    @Test
    void allConditionsMet_when_object_key_has_no_directory_manifest_key_is_just_the_file_name() {
        final String rootObjectKey = "rootfile.out";
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, null);
        stubManifest(MANIFEST_FILE_NAME, "{\"totalRecordCount\":1,\"processedRecordCount\":1}");
        when(expressionEvaluator.evaluateConditional(any(), any())).thenReturn(true);

        objectUnderTest.allConditionsMet(BUCKET, rootObjectKey, List.of(condition));

        final ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(requestCaptor.capture());
        assertThat(requestCaptor.getValue().key(), equalTo(MANIFEST_FILE_NAME));
    }

    // -------------------------------------------------------------------------
    // allConditionsMet — error cases
    // -------------------------------------------------------------------------

    @Test
    void allConditionsMet_when_manifest_file_not_found_then_returns_false() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("Not Found").build());

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(condition)), is(false));
        verify(expressionEvaluator, never()).evaluateConditional(any(), any());
    }

    @Test
    void allConditionsMet_when_s3_read_throws_unexpected_exception_then_returns_false() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(new RuntimeException("S3 connectivity error"));

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(condition)), is(false));
        verify(expressionEvaluator, never()).evaluateConditional(any(), any());
    }

    // -------------------------------------------------------------------------
    // allConditionsMet — multiple conditions
    // -------------------------------------------------------------------------

    @Test
    void allConditionsMet_when_all_applicable_conditions_pass_then_returns_true() {
        final String whenA = "/fieldA == /fieldB";
        final String whenB = "/fieldC == /fieldD";
        final S3ScanProcessingCondition conditionA = conditionWithPrefix("manifestA.out", whenA, List.of("output/"));
        final S3ScanProcessingCondition conditionB = conditionWithPrefix("manifestB.out", whenB, List.of("output/"));

        stubManifest("output/job-123/manifestA.out", "{\"fieldA\":1,\"fieldB\":1}");
        stubManifest("output/job-123/manifestB.out", "{\"fieldC\":2,\"fieldD\":2}");
        when(expressionEvaluator.evaluateConditional(eq(whenA), any(Event.class))).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(whenB), any(Event.class))).thenReturn(true);

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(conditionA, conditionB)), is(true));
    }

    @Test
    void allConditionsMet_when_first_condition_fails_then_returns_false_without_evaluating_second() {
        final S3ScanProcessingCondition conditionA = conditionWithPrefix("manifestA.out", WHEN_EXPRESSION, List.of("output/"));
        final S3ScanProcessingCondition conditionB = conditionWithPrefix("manifestB.out", "/x == /y", List.of("output/"));

        stubManifest("output/job-123/manifestA.out", "{\"totalRecordCount\":10,\"processedRecordCount\":5}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(false);

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(conditionA, conditionB)), is(false));
        verify(expressionEvaluator, never()).evaluateConditional(eq("/x == /y"), any());
    }

    @Test
    void allConditionsMet_skips_non_matching_condition_and_evaluates_matching_one() {
        final S3ScanProcessingCondition nonMatching = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("different-prefix/"));
        final S3ScanProcessingCondition matching = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));

        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":100,\"processedRecordCount\":100}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(true);

        assertThat(objectUnderTest.allConditionsMet(BUCKET, OBJECT_KEY, List.of(nonMatching, matching)), is(true));
        // Only one S3 read — the non-matching condition was skipped
        verify(s3Client).getObject(any(GetObjectRequest.class));
    }

    // -------------------------------------------------------------------------
    // findFirstMatching
    // -------------------------------------------------------------------------

    @Nested
    class FindFirstMatching {

        @Test
        void when_conditions_is_null_then_returns_null() {
            assertThat(objectUnderTest.findFirstMatching(OBJECT_KEY, null), is(nullValue()));
        }

        @Test
        void when_conditions_is_empty_then_returns_null() {
            assertThat(objectUnderTest.findFirstMatching(OBJECT_KEY, Collections.emptyList()), is(nullValue()));
        }

        @Test
        void when_no_condition_matches_then_returns_null() {
            final S3ScanProcessingCondition condition = conditionWithPrefix(
                    MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("no-match/"));
            assertThat(objectUnderTest.findFirstMatching(OBJECT_KEY, List.of(condition)), is(nullValue()));
        }

        @Test
        void when_first_condition_matches_then_returns_it() {
            final S3ScanProcessingCondition condition = conditionWithPrefix(
                    MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));
            assertThat(objectUnderTest.findFirstMatching(OBJECT_KEY, List.of(condition)), sameInstance(condition));
        }

        @Test
        void when_condition_has_no_include_prefix_it_matches_any_object_key() {
            final S3ScanProcessingCondition condition = conditionWithPrefix(
                    MANIFEST_FILE_NAME, WHEN_EXPRESSION, null);
            assertThat(objectUnderTest.findFirstMatching(OBJECT_KEY, List.of(condition)), sameInstance(condition));
        }

        @Test
        void when_first_condition_does_not_match_returns_second_matching_condition() {
            final S3ScanProcessingCondition noMatch = conditionWithPrefix(
                    MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("no-match/"));
            final S3ScanProcessingCondition match = conditionWithPrefix(
                    MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));

            assertThat(objectUnderTest.findFirstMatching(OBJECT_KEY, List.of(noMatch, match)), sameInstance(match));
        }

        @Test
        void returns_retry_settings_from_matched_condition() {
            final S3ScanProcessingCondition condition = conditionWithPrefixAndRetry(
                    MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"), Duration.ofMinutes(3), 7);

            final S3ScanProcessingCondition result = objectUnderTest.findFirstMatching(OBJECT_KEY, List.of(condition));

            assertThat(result.getRetryDelay(), equalTo(Duration.ofMinutes(3)));
            assertThat(result.getMaxRetry(), equalTo(7));
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private S3ScanProcessingCondition conditionWithPrefix(final String fileName,
                                                          final String when,
                                                          final List<String> includePrefix) {
        return conditionWithPrefixAndRetry(fileName, when, includePrefix, Duration.ofMinutes(5), 10);
    }

    private S3ScanProcessingCondition conditionWithPrefixAndRetry(final String fileName,
                                                                   final String when,
                                                                   final List<String> includePrefix,
                                                                   final Duration retryDelay,
                                                                   final int maxRetry) {
        final S3ScanProcessingCondition condition = new S3ScanProcessingCondition();
        condition.setFileName(fileName);
        condition.setWhen(when);
        condition.setIncludePrefix(includePrefix);
        condition.setRetryDelay(retryDelay);
        condition.setMaxRetry(maxRetry);
        return condition;
    }

    private void stubManifest(final String key, final String jsonContent) {
        final byte[] bytes = jsonContent.getBytes(StandardCharsets.UTF_8);
        final ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                AbortableInputStream.create(new ByteArrayInputStream(bytes)));
        when(s3Client.getObject(GetObjectRequest.builder().bucket(BUCKET).key(key).build()))
                .thenReturn(responseInputStream);
    }
}
