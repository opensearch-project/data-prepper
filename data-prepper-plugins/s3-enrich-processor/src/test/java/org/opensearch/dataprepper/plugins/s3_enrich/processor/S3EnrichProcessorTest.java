/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.expression.ExpressionParsingException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.s3.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.s3.common.source.S3ObjectReference;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.cache.S3EnricherCacheService;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.configuration.S3EnrichBucketOption;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.s3source.S3ObjectReferenceResolver;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.s3source.S3ObjectWorker;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class S3EnrichProcessorTest {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String KEY_PATH = "s3_key";
    private static final String CORRELATION_KEY = "id";
    private static final String TAG_ON_FAILURE = "s3_enrich_failed";

    @Mock
    private S3EnrichProcessorConfig s3EnrichProcessorConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private S3EnricherCacheService cacheService;

    @Mock
    private S3ObjectWorker s3ObjectWorker;

    @Mock
    private S3ObjectReferenceResolver s3ObjectReferenceResolver;

    @Mock
    private Counter successCounter;

    @Mock
    private Counter failureCounter;

    private S3EnrichProcessor objectUnderTest;

    @BeforeEach
    void setUp() throws Exception {
        // --- PluginMetrics stubs ---
        final Counter genericCounter = mock(Counter.class);
        final Timer genericTimer = mock(Timer.class);
        final DistributionSummary genericSummary = mock(DistributionSummary.class);
        when(pluginMetrics.counter(anyString())).thenReturn(genericCounter);
        when(pluginMetrics.timer(anyString())).thenReturn(genericTimer);
        when(pluginMetrics.summary(anyString())).thenReturn(genericSummary);
        // Return specific counters for the two we care about
        when(pluginMetrics.counter(S3EnrichProcessor.NUMBER_OF_RECORDS_ENRICHED_SUCCESS)).thenReturn(successCounter);
        when(pluginMetrics.counter(S3EnrichProcessor.NUMBER_OF_RECORDS_ENRICHED_FAILED)).thenReturn(failureCounter);

        // --- Config stubs ---
        final AwsAuthenticationOptions awsAuthOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsAuthOptions.getAwsStsRoleArn()).thenReturn(null);
        when(awsAuthOptions.getAwsStsHeaderOverrides()).thenReturn(Collections.emptyMap());
        when(awsAuthOptions.getAwsStsExternalId()).thenReturn(null);

        final S3EnrichBucketOption bucketOption = mock(S3EnrichBucketOption.class);
        when(bucketOption.getName()).thenReturn(BUCKET_NAME);
        when(bucketOption.getS3SourceFilter()).thenReturn(null);

        final PluginModel codec = mock(PluginModel.class);
        when(codec.getPluginName()).thenReturn("newline_delimited");
        when(codec.getPluginSettings()).thenReturn(Collections.emptyMap());

        when(s3EnrichProcessorConfig.getCacheTtl()).thenReturn(java.time.Duration.ofMinutes(10));
        when(s3EnrichProcessorConfig.getCacheSizeLimit()).thenReturn(1000);
        when(s3EnrichProcessorConfig.getEnricherNamePattern()).thenReturn("^(.*)_output\\.jsonl$");
        when(s3EnrichProcessorConfig.getTagsOnFailure()).thenReturn(List.of(TAG_ON_FAILURE));
        when(s3EnrichProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthOptions);
        when(s3EnrichProcessorConfig.isDisableBucketOwnershipValidation()).thenReturn(true);
        when(s3EnrichProcessorConfig.getCodec()).thenReturn(codec);
        when(s3EnrichProcessorConfig.getEnricherKeyPath()).thenReturn(KEY_PATH);
        when(s3EnrichProcessorConfig.getCorrelationKeys()).thenReturn(List.of(CORRELATION_KEY));
        when(s3EnrichProcessorConfig.getS3EnrichBucketOption()).thenReturn(bucketOption);
        when(s3EnrichProcessorConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(s3EnrichProcessorConfig.getWhenCondition()).thenReturn(null);
        when(s3EnrichProcessorConfig.getMergeKeys()).thenReturn(Collections.emptyList());
        when(s3EnrichProcessorConfig.getS3IncludePrefix()).thenReturn(java.util.Optional.empty());

        // --- AWS credential stubs ---
        final AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(credentialsProvider);

        // --- Plugin factory stub ---
        when(pluginFactory.loadPlugin(any(), any())).thenReturn(mock(org.opensearch.dataprepper.model.codec.InputCodec.class));

        objectUnderTest = new S3EnrichProcessor(
                s3EnrichProcessorConfig,
                pluginMetrics,
                awsCredentialsSupplier,
                expressionEvaluator,
                pluginFactory);

        // Inject mocked dependencies via reflection
        injectField(objectUnderTest, "cacheService", cacheService);
        injectField(objectUnderTest, "s3ObjectWorker", s3ObjectWorker);
        injectField(objectUnderTest, "s3ObjectReferenceResolver", s3ObjectReferenceResolver);
        injectField(objectUnderTest, "numberOfRecordsSuccessCounter", successCounter);
        injectField(objectUnderTest, "numberOfRecordsFailedCounter", failureCounter);
    }

    // ---- doExecute: when-condition filtering ----

    @Test
    void doExecute_returns_all_records_when_collection_is_empty() {
        final Collection<Record<Event>> result = objectUnderTest.doExecute(Collections.emptyList());

        assertThat(result.size(), equalTo(0));
    }

    @Test
    void doExecute_processes_all_records_when_no_when_condition_configured() throws Exception {
        when(s3EnrichProcessorConfig.getWhenCondition()).thenReturn(null);
        final Record<Event> record = mockRecord("s3://bucket/key", "123");
        final Event event = record.getData();
        final Event enrichEvent = mock(Event.class);
        final S3ObjectReference ref = S3ObjectReference.bucketAndKey(BUCKET_NAME, "key").build();

        when(s3ObjectReferenceResolver.resolve(event)).thenReturn(ref);
        when(event.get(CORRELATION_KEY, String.class)).thenReturn("123");
        when(cacheService.get(ref.uri(), "123")).thenReturn(enrichEvent);

        final EventKey mergeKey = mock(EventKey.class);
        when(mergeKey.getKey()).thenReturn("field1");
        when(s3EnrichProcessorConfig.getMergeKeys()).thenReturn(List.of(mergeKey));
        when(enrichEvent.containsKey("field1")).thenReturn(true);
        when(enrichEvent.get("field1", Object.class)).thenReturn("value1");

        final Collection<Record<Event>> result = objectUnderTest.doExecute(List.of(record));

        assertThat(result.size(), equalTo(1));
        verify(successCounter).increment();
    }

    @Test
    void doExecute_passes_through_records_not_meeting_when_condition() {
        when(s3EnrichProcessorConfig.getWhenCondition()).thenReturn("/some_key != null");
        final Record<Event> record = mockRecord("s3://bucket/key", "123");
        when(expressionEvaluator.evaluateConditional("/some_key != null", record.getData())).thenReturn(false);

        final Collection<Record<Event>> result = objectUnderTest.doExecute(List.of(record));

        assertThat(result.size(), equalTo(1));
        verify(successCounter, never()).increment();
        verify(failureCounter, never()).increment();
    }

    @Test
    void doExecute_returns_record_unmodified_when_expression_parsing_fails() {
        when(s3EnrichProcessorConfig.getWhenCondition()).thenReturn("/bad_expr");
        final Record<Event> record = mockRecord("s3://bucket/key", "123");
        when(expressionEvaluator.evaluateConditional(anyString(), any(Event.class)))
                .thenThrow(new ExpressionParsingException("bad expression", new RuntimeException()));

        final Collection<Record<Event>> result = objectUnderTest.doExecute(List.of(record));

        assertThat(result.size(), equalTo(1));
        verify(s3ObjectReferenceResolver, never()).resolve(any());
    }

    @Test
    void doExecute_returns_record_unmodified_when_class_cast_exception_during_condition_eval() {
        when(s3EnrichProcessorConfig.getWhenCondition()).thenReturn("/some_key");
        final Record<Event> record = mockRecord("s3://bucket/key", "123");
        when(expressionEvaluator.evaluateConditional(anyString(), any(Event.class)))
                .thenThrow(new ClassCastException("unexpected type"));

        final Collection<Record<Event>> result = objectUnderTest.doExecute(List.of(record));

        assertThat(result.size(), equalTo(1));
        verify(s3ObjectReferenceResolver, never()).resolve(any());
    }

    @Test
    void doExecute_returns_record_unmodified_when_unexpected_exception_during_condition_eval() {
        when(s3EnrichProcessorConfig.getWhenCondition()).thenReturn("/some_key");
        final Record<Event> record = mockRecord("s3://bucket/key", "123");
        when(expressionEvaluator.evaluateConditional(anyString(), any(Event.class)))
                .thenThrow(new RuntimeException("unexpected"));

        final Collection<Record<Event>> result = objectUnderTest.doExecute(List.of(record));

        assertThat(result.size(), equalTo(1));
        verify(s3ObjectReferenceResolver, never()).resolve(any());
    }

    // ---- processRecord: correlation and merge ----

    @Test
    void doExecute_adds_failure_tag_and_increments_counter_when_process_record_throws() {
        when(s3EnrichProcessorConfig.getWhenCondition()).thenReturn(null);
        final Record<Event> record = mockRecord("s3://bucket/key", "123");
        final Event event = record.getData();
        final S3ObjectReference ref = S3ObjectReference.bucketAndKey(BUCKET_NAME, "key").build();
        when(s3ObjectReferenceResolver.resolve(event)).thenReturn(ref);
        when(event.get(CORRELATION_KEY, String.class)).thenReturn(null); // triggers exception in processRecord

        final Collection<Record<Event>> result = objectUnderTest.doExecute(List.of(record));

        assertThat(result.size(), equalTo(1));
        verify(failureCounter).increment();
        verify(successCounter, never()).increment();
        final EventMetadata metadata = event.getMetadata();
        verify(metadata).addTags(List.of(TAG_ON_FAILURE));
    }

    @Test
    void doExecute_increments_failure_counter_when_no_matching_enrich_event_in_cache() {
        when(s3EnrichProcessorConfig.getWhenCondition()).thenReturn(null);
        final Record<Event> record = mockRecord("s3://bucket/key", "123");
        final Event event = record.getData();
        final S3ObjectReference ref = S3ObjectReference.bucketAndKey(BUCKET_NAME, "key").build();
        when(s3ObjectReferenceResolver.resolve(event)).thenReturn(ref);
        when(event.get(CORRELATION_KEY, String.class)).thenReturn("123");
        when(cacheService.get(ref.uri(), "123")).thenReturn(null); // no matching event

        final Collection<Record<Event>> result = objectUnderTest.doExecute(List.of(record));

        assertThat(result.size(), equalTo(1));
        verify(failureCounter).increment();
    }

    @Test
    void doExecute_succeeds_and_merges_fields_when_enrich_event_found_in_cache() {
        when(s3EnrichProcessorConfig.getWhenCondition()).thenReturn(null);
        final Record<Event> record = mockRecord("s3://bucket/key", "123");
        final Event event = record.getData();
        final Event enrichEvent = mock(Event.class);
        final S3ObjectReference ref = S3ObjectReference.bucketAndKey(BUCKET_NAME, "key").build();
        when(s3ObjectReferenceResolver.resolve(event)).thenReturn(ref);
        when(event.get(CORRELATION_KEY, String.class)).thenReturn("123");
        when(cacheService.get(ref.uri(), "123")).thenReturn(enrichEvent);

        final EventKey mergeKey = mock(EventKey.class);
        when(mergeKey.getKey()).thenReturn("city");
        when(s3EnrichProcessorConfig.getMergeKeys()).thenReturn(List.of(mergeKey));
        when(enrichEvent.containsKey("city")).thenReturn(true);
        when(enrichEvent.get("city", Object.class)).thenReturn("Seattle");

        final Collection<Record<Event>> result = objectUnderTest.doExecute(List.of(record));

        assertThat(result.size(), equalTo(1));
        verify(successCounter).increment();
        verify(event).put(mergeKey, "Seattle");
    }

    @Test
    void doExecute_partial_merge_failure_logs_warning_but_does_not_increment_failure_counter() {
        when(s3EnrichProcessorConfig.getWhenCondition()).thenReturn(null);
        final Record<Event> record = mockRecord("s3://bucket/key", "123");
        final Event event = record.getData();
        final Event enrichEvent = mock(Event.class);
        final S3ObjectReference ref = S3ObjectReference.bucketAndKey(BUCKET_NAME, "key").build();
        when(s3ObjectReferenceResolver.resolve(event)).thenReturn(ref);
        when(event.get(CORRELATION_KEY, String.class)).thenReturn("123");
        when(cacheService.get(ref.uri(), "123")).thenReturn(enrichEvent);

        final EventKey goodKey = mock(EventKey.class);
        when(goodKey.getKey()).thenReturn("good_field");
        final EventKey missingKey = mock(EventKey.class);
        when(missingKey.getKey()).thenReturn("missing_field");
        when(s3EnrichProcessorConfig.getMergeKeys()).thenReturn(List.of(goodKey, missingKey));
        when(enrichEvent.containsKey("good_field")).thenReturn(true);
        when(enrichEvent.get("good_field", Object.class)).thenReturn("value");
        when(enrichEvent.containsKey("missing_field")).thenReturn(false); // this key fails

        final Collection<Record<Event>> result = objectUnderTest.doExecute(List.of(record));

        assertThat(result.size(), equalTo(1));
        verify(successCounter).increment();
        verify(failureCounter, never()).increment();
    }

    // ---- addFailureTags ----

    @Test
    void addFailureTags_adds_configured_tags_to_event_metadata() {
        final Record<Event> record = mockRecord("s3://bucket/key", "id-val");
        final Event event = record.getData();
        final EventMetadata metadata = event.getMetadata();

        objectUnderTest.addFailureTags(record);

        verify(metadata).addTags(List.of(TAG_ON_FAILURE));
    }

    @Test
    void addFailureTags_does_nothing_when_tags_on_failure_is_empty() throws Exception {
        when(s3EnrichProcessorConfig.getTagsOnFailure()).thenReturn(Collections.emptyList());
        // re-inject tagsOnFailure via reflection
        injectField(objectUnderTest, "tagsOnFailure", Collections.emptyList());
        final Record<Event> record = mockRecord("s3://bucket/key", "id-val");
        final Event event = record.getData();

        objectUnderTest.addFailureTags(record);

        verify(event.getMetadata(), never()).addTags(any());
    }

    @Test
    void addFailureTags_list_overload_adds_tags_to_all_records() {
        final Record<Event> record1 = mockRecord("s3://bucket/key", "id-1");
        final Record<Event> record2 = mockRecord("s3://bucket/key", "id-2");

        objectUnderTest.addFailureTags(List.of(record1, record2));

        verify(record1.getData().getMetadata()).addTags(List.of(TAG_ON_FAILURE));
        verify(record2.getData().getMetadata()).addTags(List.of(TAG_ON_FAILURE));
    }

    @Test
    void addFailureTags_list_overload_does_nothing_for_empty_list() {
        objectUnderTest.addFailureTags(Collections.emptyList());
        // no exception, no interactions
    }

    // ---- lifecycle ----

    @Test
    void isReadyForShutdown_returns_true() {
        assertThat(objectUnderTest.isReadyForShutdown(), equalTo(true));
    }

    @Test
    void prepareForShutdown_does_not_throw() {
        objectUnderTest.prepareForShutdown();
    }

    @Test
    void shutdown_does_not_throw() {
        objectUnderTest.shutdown();
    }

    // ---- helpers ----

    private Record<Event> mockRecord(final String s3Uri, final String eventId) {
        final Event event = mock(Event.class);
        final EventMetadata metadata = mock(EventMetadata.class);
        when(event.getMetadata()).thenReturn(metadata);
        @SuppressWarnings("unchecked")
        final Record<Event> record = mock(Record.class);
        when(record.getData()).thenReturn(event);
        return record;
    }

    private static void injectField(final Object target, final String fieldName, final Object value) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                final Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (final NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException("Field '" + fieldName + "' not found in class hierarchy of " + target.getClass().getName());
    }
}
