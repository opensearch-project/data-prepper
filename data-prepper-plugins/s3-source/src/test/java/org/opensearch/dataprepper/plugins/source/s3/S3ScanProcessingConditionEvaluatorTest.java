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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanProcessingCondition;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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

    @Mock
    private PluginFactory pluginFactory;

    private S3ScanProcessingConditionEvaluator objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new S3ScanProcessingConditionEvaluator(
                s3Client, expressionEvaluator, pluginFactory, Collections.emptyList());
    }

    // -------------------------------------------------------------------------
    // firstUnmetCondition — null / empty guards
    // -------------------------------------------------------------------------

    @Test
    void firstUnmetCondition_when_conditions_is_null_then_returns_empty() {
        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, null), is(Optional.empty()));
        verify(s3Client, never()).getObject(any(GetObjectRequest.class));
    }

    @Test
    void firstUnmetCondition_when_conditions_is_empty_then_returns_empty() {
        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, Collections.emptyList()), is(Optional.empty()));
        verify(s3Client, never()).getObject(any(GetObjectRequest.class));
    }

    // -------------------------------------------------------------------------
    // firstUnmetCondition — applicable_prefix filtering
    // -------------------------------------------------------------------------

    @Test
    void firstUnmetCondition_when_object_key_does_not_match_applicable_prefix_then_condition_is_skipped() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("different-prefix/"));

        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition)), is(Optional.empty()));
        verify(s3Client, never()).getObject(any(GetObjectRequest.class));
    }

    @Test
    void firstUnmetCondition_when_object_key_matches_one_of_multiple_prefixes_then_condition_applies() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("no-match/", "output/"));
        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":50,\"processedRecordCount\":50}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(true);

        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition)), is(Optional.empty()));
    }

    // -------------------------------------------------------------------------
    // firstUnmetCondition — expression evaluation
    // -------------------------------------------------------------------------

    @Test
    void firstUnmetCondition_when_expression_evaluates_to_false_then_returns_that_condition() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));
        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":200,\"processedRecordCount\":100}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(false);

        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition)),
                is(Optional.of(condition)));
    }

    @Test
    void firstUnmetCondition_passes_manifest_json_fields_to_expression_evaluator() {
        final String manifestJson = "{\"totalRecordCount\":50000,\"processedRecordCount\":8088}";
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, null);
        stubManifest(MANIFEST_KEY, manifestJson);

        final ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), eventCaptor.capture())).thenReturn(true);

        objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition));

        final Event capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.get("totalRecordCount", Integer.class), equalTo(50000));
        assertThat(capturedEvent.get("processedRecordCount", Integer.class), equalTo(8088));
    }

    // -------------------------------------------------------------------------
    // firstUnmetCondition — manifest key resolution
    // -------------------------------------------------------------------------

    @Test
    void firstUnmetCondition_manifest_key_uses_same_directory_as_object_key() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, null);
        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":1,\"processedRecordCount\":1}");
        when(expressionEvaluator.evaluateConditional(any(), any())).thenReturn(true);

        objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition));

        final ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(requestCaptor.capture());
        assertThat(requestCaptor.getValue().key(), equalTo(MANIFEST_KEY));
        assertThat(requestCaptor.getValue().bucket(), equalTo(BUCKET));
    }

    @Test
    void firstUnmetCondition_when_object_key_has_no_directory_manifest_key_is_just_the_file_name() {
        final String rootObjectKey = "rootfile.out";
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, null);
        stubManifest(MANIFEST_FILE_NAME, "{\"totalRecordCount\":1,\"processedRecordCount\":1}");
        when(expressionEvaluator.evaluateConditional(any(), any())).thenReturn(true);

        objectUnderTest.firstUnmetCondition(BUCKET, rootObjectKey, List.of(condition));

        final ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3Client).getObject(requestCaptor.capture());
        assertThat(requestCaptor.getValue().key(), equalTo(MANIFEST_FILE_NAME));
    }

    // -------------------------------------------------------------------------
    // firstUnmetCondition — error cases
    // -------------------------------------------------------------------------

    @Test
    void firstUnmetCondition_when_manifest_file_not_found_then_returns_that_condition() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("Not Found").build());

        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition)),
                is(Optional.of(condition)));
        verify(expressionEvaluator, never()).evaluateConditional(any(), any());
    }

    @Test
    void firstUnmetCondition_when_s3_read_throws_unexpected_exception_then_returns_empty() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(new RuntimeException("S3 connectivity error"));

        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition)), is(Optional.empty()));
        verify(expressionEvaluator, never()).evaluateConditional(any(), any());
    }

    // -------------------------------------------------------------------------
    // firstUnmetCondition — multiple conditions
    // -------------------------------------------------------------------------

    @Test
    void firstUnmetCondition_when_all_applicable_conditions_pass_then_returns_empty() {
        final String whenA = "/fieldA == /fieldB";
        final String whenB = "/fieldC == /fieldD";
        final S3ScanProcessingCondition conditionA = conditionWithPrefix("manifestA.out", whenA, List.of("output/"));
        final S3ScanProcessingCondition conditionB = conditionWithPrefix("manifestB.out", whenB, List.of("output/"));

        stubManifest("output/job-123/manifestA.out", "{\"fieldA\":1,\"fieldB\":1}");
        stubManifest("output/job-123/manifestB.out", "{\"fieldC\":2,\"fieldD\":2}");
        when(expressionEvaluator.evaluateConditional(eq(whenA), any(Event.class))).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(whenB), any(Event.class))).thenReturn(true);

        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(conditionA, conditionB)),
                is(Optional.empty()));
    }

    @Test
    void firstUnmetCondition_when_first_condition_fails_returns_it_without_evaluating_second() {
        final S3ScanProcessingCondition conditionA = conditionWithPrefix("manifestA.out", WHEN_EXPRESSION, List.of("output/"));
        final S3ScanProcessingCondition conditionB = conditionWithPrefix("manifestB.out", "/x == /y", List.of("output/"));

        stubManifest("output/job-123/manifestA.out", "{\"totalRecordCount\":10,\"processedRecordCount\":5}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(false);

        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(conditionA, conditionB)),
                is(Optional.of(conditionA)));
        verify(expressionEvaluator, never()).evaluateConditional(eq("/x == /y"), any());
    }

    @Test
    void firstUnmetCondition_when_first_condition_passes_and_second_fails_returns_second() {
        final String whenA = "/fieldA == /fieldB";
        final String whenB = "/fieldC == /fieldD";
        final S3ScanProcessingCondition conditionA = conditionWithPrefix("manifestA.out", whenA, List.of("output/"));
        final S3ScanProcessingCondition conditionB = conditionWithPrefix("manifestB.out", whenB, List.of("output/"));

        stubManifest("output/job-123/manifestA.out", "{\"fieldA\":1,\"fieldB\":1}");
        stubManifest("output/job-123/manifestB.out", "{\"fieldC\":1,\"fieldD\":2}");
        when(expressionEvaluator.evaluateConditional(eq(whenA), any(Event.class))).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(whenB), any(Event.class))).thenReturn(false);

        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(conditionA, conditionB)),
                is(Optional.of(conditionB)));
    }

    @Test
    void firstUnmetCondition_skips_non_matching_condition_and_evaluates_matching_one() {
        final S3ScanProcessingCondition nonMatching = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("different-prefix/"));
        final S3ScanProcessingCondition matching = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));

        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":100,\"processedRecordCount\":100}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(true);

        assertThat(objectUnderTest.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(nonMatching, matching)),
                is(Optional.empty()));
        // Only one S3 read — the non-matching condition was skipped
        verify(s3Client).getObject(any(GetObjectRequest.class));
    }

    // -------------------------------------------------------------------------
    // firstUnmetCondition — codec path
    // -------------------------------------------------------------------------

    @Test
    void firstUnmetCondition_when_codec_configured_and_expression_false_returns_condition() throws Exception {
        final S3ScanProcessingCondition condition = conditionWithCodec(MANIFEST_FILE_NAME, WHEN_EXPRESSION, null, "json");
        final InputCodec codec = mockCodecProducingEvent(Map.of("totalRecordCount", 100, "processedRecordCount", 50));
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any(PluginSetting.class))).thenReturn(codec);
        final S3ScanProcessingConditionEvaluator evaluator = evaluatorWith(condition);

        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":100,\"processedRecordCount\":50}");
        when(expressionEvaluator.evaluateConditional(eq(WHEN_EXPRESSION), any(Event.class))).thenReturn(false);

        assertThat(evaluator.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition)),
                is(Optional.of(condition)));
    }

    @Test
    void firstUnmetCondition_when_codec_produces_no_events_returns_condition() throws Exception {
        final S3ScanProcessingCondition condition = conditionWithCodec(MANIFEST_FILE_NAME, WHEN_EXPRESSION, null, "json");
        final InputCodec noEventCodec = mockCodecProducingNoEvents();
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any(PluginSetting.class))).thenReturn(noEventCodec);
        final S3ScanProcessingConditionEvaluator evaluator = evaluatorWith(condition);

        stubManifest(MANIFEST_KEY, "");

        assertThat(evaluator.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition)),
                is(Optional.of(condition)));
        verify(expressionEvaluator, never()).evaluateConditional(any(), any());
    }

    @Test
    void firstUnmetCondition_codec_loaded_once_at_construction_not_per_object() throws Exception {
        final S3ScanProcessingCondition condition = conditionWithCodec(MANIFEST_FILE_NAME, WHEN_EXPRESSION, null, "json");
        final InputCodec codec = mockCodecProducingEvent(Map.of("totalRecordCount", 100, "processedRecordCount", 100));
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any(PluginSetting.class))).thenReturn(codec);
        final S3ScanProcessingConditionEvaluator evaluator = evaluatorWith(condition);

        stubManifest(MANIFEST_KEY, "{\"totalRecordCount\":100,\"processedRecordCount\":100}");
        when(expressionEvaluator.evaluateConditional(any(), any())).thenReturn(true);

        evaluator.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition));
        evaluator.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition));
        evaluator.firstUnmetCondition(BUCKET, OBJECT_KEY, List.of(condition));

        // loadPlugin called exactly once during construction regardless of how many objects are evaluated
        verify(pluginFactory).loadPlugin(eq(InputCodec.class), any(PluginSetting.class));
    }

    @Test
    void firstUnmetCondition_without_codec_does_not_call_plugin_factory() {
        final S3ScanProcessingCondition condition = conditionWithPrefix(
                MANIFEST_FILE_NAME, WHEN_EXPRESSION, List.of("output/"));

        new S3ScanProcessingConditionEvaluator(s3Client, expressionEvaluator, pluginFactory, List.of(condition));

        verify(pluginFactory, never()).loadPlugin(any(), any(PluginSetting.class));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private S3ScanProcessingConditionEvaluator evaluatorWith(final S3ScanProcessingCondition... conditions) {
        return new S3ScanProcessingConditionEvaluator(s3Client, expressionEvaluator, pluginFactory, List.of(conditions));
    }

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
        condition.setObjectName(fileName);
        condition.setWhen(when);
        condition.setApplicablePrefix(includePrefix);
        condition.setRetryDelay(retryDelay);
        condition.setMaxRetry(maxRetry);
        return condition;
    }

    private S3ScanProcessingCondition conditionWithCodec(final String fileName,
                                                         final String when,
                                                         final List<String> includePrefix,
                                                         final String codecName) {
        final S3ScanProcessingCondition condition = conditionWithPrefix(fileName, when, includePrefix);
        final PluginModel codecModel = new PluginModel(codecName, Collections.emptyMap());
        condition.setCodec(codecModel);
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

    @SuppressWarnings("unchecked")
    private InputCodec mockCodecProducingEvent(final Map<String, Object> data) throws Exception {
        final InputCodec codec = org.mockito.Mockito.mock(InputCodec.class);
        doAnswer(invocation -> {
            final Consumer<Record<Event>> consumer = invocation.getArgument(1);
            final Event event = org.opensearch.dataprepper.model.event.JacksonEvent.builder()
                    .withEventType("event")
                    .withData(data)
                    .build();
            consumer.accept(new Record<>(event));
            return null;
        }).when(codec).parse(any(InputStream.class), any(Consumer.class));
        return codec;
    }

    @SuppressWarnings("unchecked")
    private InputCodec mockCodecProducingNoEvents() throws Exception {
        final InputCodec codec = org.mockito.Mockito.mock(InputCodec.class);
        doAnswer(invocation -> null).when(codec).parse(any(InputStream.class), any(Consumer.class));
        return codec;
    }
}
