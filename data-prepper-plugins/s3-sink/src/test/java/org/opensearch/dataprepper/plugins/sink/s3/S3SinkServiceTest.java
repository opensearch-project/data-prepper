/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AggregateThresholdOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ThresholdOptions;
import org.opensearch.dataprepper.plugins.sink.s3.grouping.S3Group;
import org.opensearch.dataprepper.plugins.sink.s3.grouping.S3GroupManager;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.s3.S3SinkService.NUMBER_OF_GROUPS_FORCE_FLUSHED;

class S3SinkServiceTest {

    public static final int MAX_EVENTS = 10;
    public static final int MAX_RETRIES = 3;
    public static final String BUCKET_NAME = UUID.randomUUID().toString();
    public static final String S3_REGION = "us-east-1";
    public static final String MAXIMUM_SIZE = "1kb";
    public static final String OBJECT_KEY_NAME_PATTERN = "my-elb-%{yyyy-MM-dd'T'hh-mm-ss}";
    public static final String CODEC_PLUGIN_NAME = "json";
    public static final String PATH_PREFIX = "logdata/";
    private S3SinkConfig s3SinkConfig;
    private S3Client s3Client;
    private OutputCodec codec;
    private OutputCodecContext codecContext;
    private KeyGenerator keyGenerator = mock(KeyGenerator.class);
    private PluginMetrics pluginMetrics;
    private Counter snapshotSuccessCounter;

    private Counter numberOfRecordsSuccessCounter;

    private Counter s3ObjectsForceFlushedCounter;
    private DistributionSummary s3ObjectSizeSummary;
    private Random random;
    private String tagsTargetKey;
    private AcknowledgementSet acknowledgementSet;

    private S3GroupManager s3GroupManager;

    @BeforeEach
    void setUp() {

        random = new Random();
        tagsTargetKey = RandomStringUtils.randomAlphabetic(5);
        s3SinkConfig = mock(S3SinkConfig.class);
        acknowledgementSet = mock(AcknowledgementSet.class);
        codecContext = new OutputCodecContext(tagsTargetKey, Collections.emptyList(), Collections.emptyList());
        s3Client = mock(S3Client.class);
        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
        ObjectKeyOptions objectKeyOptions = mock(ObjectKeyOptions.class);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        PluginSetting pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        PluginModel pluginModel = mock(PluginModel.class);
        PluginFactory pluginFactory = mock(PluginFactory.class);
        codec = mock(OutputCodec.class);
        snapshotSuccessCounter = mock(Counter.class);
        numberOfRecordsSuccessCounter = mock(Counter.class);
        Counter snapshotFailedCounter = mock(Counter.class);
        Counter numberOfRecordsFailedCounter = mock(Counter.class);
        s3ObjectSizeSummary = mock(DistributionSummary.class);
        s3ObjectsForceFlushedCounter = mock(Counter.class);

        s3GroupManager = mock(S3GroupManager.class);

        when(objectKeyOptions.getNamePattern()).thenReturn(OBJECT_KEY_NAME_PATTERN);
        when(s3SinkConfig.getMaxUploadRetries()).thenReturn(MAX_RETRIES);
        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(MAX_EVENTS);
        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse(MAXIMUM_SIZE));
        when(s3SinkConfig.getThresholdOptions().getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(5));
        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.INMEMORY);
        when(s3SinkConfig.getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(s3SinkConfig.getBucketName()).thenReturn(BUCKET_NAME);
        when(s3SinkConfig.getObjectKeyOptions().getPathPrefix()).thenReturn(PATH_PREFIX);
        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(S3_REGION));
        when(s3SinkConfig.getCodec()).thenReturn(pluginModel);
        when(pluginModel.getPluginName()).thenReturn(CODEC_PLUGIN_NAME);
        when(pluginFactory.loadPlugin(OutputCodec.class, pluginSetting)).thenReturn(codec);

        lenient().when(pluginMetrics.counter(S3SinkService.OBJECTS_SUCCEEDED)).thenReturn(snapshotSuccessCounter);
        lenient().when(pluginMetrics.counter(S3SinkService.OBJECTS_FAILED)).thenReturn(snapshotFailedCounter);
        lenient().when(pluginMetrics.counter(S3SinkService.NUMBER_OF_RECORDS_FLUSHED_TO_S3_SUCCESS)).
                thenReturn(numberOfRecordsSuccessCounter);
        lenient().when(pluginMetrics.counter(S3SinkService.NUMBER_OF_RECORDS_FLUSHED_TO_S3_FAILED)).
                thenReturn(numberOfRecordsFailedCounter);
        lenient().when(pluginMetrics.summary(S3SinkService.S3_OBJECTS_SIZE)).thenReturn(s3ObjectSizeSummary);

        lenient().when(pluginMetrics.counter(NUMBER_OF_GROUPS_FORCE_FLUSHED)).thenReturn(s3ObjectsForceFlushedCounter);

        final AggregateThresholdOptions aggregateThresholdOptions = mock(AggregateThresholdOptions.class);
        when(aggregateThresholdOptions.getMaximumSize()).thenReturn(ByteCount.ofBytes(Integer.MAX_VALUE));
        when(s3SinkConfig.getAggregateThresholdOptions()).thenReturn(aggregateThresholdOptions);
        when(s3GroupManager.recalculateAndGetGroupSize()).thenReturn(100_000L);
    }

    private DefaultEventHandle castToDefaultHandle(EventHandle eventHandle) {
        return (DefaultEventHandle)eventHandle;
    }

    private S3SinkService createObjectUnderTest() {
        return new S3SinkService(s3SinkConfig, codecContext, Duration.ofMillis(100), pluginMetrics, s3GroupManager);
    }

    @Test
    void test_s3SinkService_notNull() {
        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));
    }

    @Test
    void test_output_with_threshold_set_as_more_then_zero_event_count() throws IOException {
        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        when(buffer.getEventCount()).thenReturn(10);
        when(buffer.getKey()).thenReturn(UUID.randomUUID().toString());

        final CompletableFuture<?> completableFuture = mock(CompletableFuture.class);
        when(buffer.flushToS3(any(Consumer.class), any(Consumer.class))).thenReturn(Optional.of(completableFuture));

        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(5);
        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());

        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getBuffer()).thenReturn(buffer);
        when(s3Group.getOutputCodec()).thenReturn(codec);

        when(s3GroupManager.getOrCreateGroupForEvent(any(Event.class))).thenReturn(s3Group);
        when(s3GroupManager.getS3GroupEntries()).thenReturn(Collections.singletonList(s3Group));
        doNothing().when(codec).writeEvent(event, outputStream);
        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);

        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            s3SinkService.output(generateRandomStringEventRecord());
        }

        final ArgumentCaptor<Consumer<Boolean>> argumentCaptorForCompletion = ArgumentCaptor.forClass(Consumer.class);
        verify(buffer, times(51)).flushToS3(argumentCaptorForCompletion.capture(), any(Consumer.class));

        final List<Consumer<Boolean>> completionConsumers = argumentCaptorForCompletion.getAllValues();
        assertThat(completionConsumers.size(), equalTo(51));

        final Consumer<Boolean> completionConsumer = completionConsumers.get(0);
        completionConsumer.accept(true);


        // only ran one of the completion consumers
        verify(snapshotSuccessCounter, times(1)).increment();
        verify(numberOfRecordsSuccessCounter).increment(s3Group.getBuffer().getEventCount());
        verify(s3Group).releaseEventHandles(true);
    }


    // If event_count threshold set as zero, Hence event_count will be
    // ignored as part of threshold check.
    @Test
    void test_output_with_threshold_set_as_zero_event_count() throws IOException {

        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        when(buffer.getSize()).thenReturn(25500L);

        final CompletableFuture<?> completableFuture = mock(CompletableFuture.class);
        when(buffer.flushToS3(any(Consumer.class), any(Consumer.class))).thenReturn(Optional.of(completableFuture));

        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(0);
        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse("2kb"));
        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getBuffer()).thenReturn(buffer);
        when(s3Group.getOutputCodec()).thenReturn(codec);

        when(s3GroupManager.getOrCreateGroupForEvent(any(Event.class))).thenReturn(s3Group);
        when(s3GroupManager.getS3GroupEntries()).thenReturn(Collections.singletonList(s3Group));
        doNothing().when(codec).writeEvent(event, outputStream);
        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            s3SinkService.output(generateRandomStringEventRecord());
        }
    }

    @Test
    void test_output_with_uploadedToS3_success() throws IOException {

        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        when(buffer.getEventCount()).thenReturn(10);
        final CompletableFuture<?> completableFuture = mock(CompletableFuture.class);
        when(buffer.flushToS3(any(Consumer.class), any(Consumer.class))).thenReturn(Optional.of(completableFuture));

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getBuffer()).thenReturn(buffer);
        when(s3Group.getOutputCodec()).thenReturn(codec);

        when(s3GroupManager.getOrCreateGroupForEvent(any(Event.class))).thenReturn(s3Group);
        when(s3GroupManager.getS3GroupEntries()).thenReturn(Collections.singletonList(s3Group));

        doNothing().when(codec).writeEvent(event, outputStream);
        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));
        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            s3SinkService.output(generateRandomStringEventRecord());
        }
    }

    @Test
    void test_output_with_uploadedToS3_success_records_byte_count() throws IOException {

        Buffer buffer = mock(Buffer.class);

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final CompletableFuture<?> completableFuture = mock(CompletableFuture.class);
        when(buffer.flushToS3(any(Consumer.class), any(Consumer.class))).thenReturn(Optional.of(completableFuture));

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getBuffer()).thenReturn(buffer);
        when(s3Group.getOutputCodec()).thenReturn(codec);

        when(s3GroupManager.getOrCreateGroupForEvent(any(Event.class))).thenReturn(s3Group);
        when(s3GroupManager.getS3GroupEntries()).thenReturn(Collections.singletonList(s3Group));

        doNothing().when(codec).writeEvent(event, outputStream);
        final S3SinkService s3SinkService = createObjectUnderTest();
        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            s3SinkService.output(generateRandomStringEventRecord());
        }
    }

    @Test
    void test_output_with_uploadedToS3_midBatch_generatesNewOutputStream() throws IOException {
        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        when(buffer.getEventCount()).thenReturn(10);
        final CompletableFuture<?> completableFuture = mock(CompletableFuture.class);
        when(buffer.flushToS3(any(Consumer.class), any(Consumer.class))).thenReturn(Optional.of(completableFuture));
        final OutputStream outputStream1 = mock(OutputStream.class);
        final OutputStream outputStream2 = mock(OutputStream.class);
        when(buffer.getOutputStream())
                .thenReturn(outputStream1)
                .thenReturn(outputStream2);

        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getBuffer()).thenReturn(buffer);
        when(s3Group.getOutputCodec()).thenReturn(codec);

        when(s3GroupManager.getOrCreateGroupForEvent(any(Event.class))).thenReturn(s3Group);
        when(s3GroupManager.getS3GroupEntries()).thenReturn(Collections.singletonList(s3Group));

        doNothing().when(codec).writeEvent(any(), eq(outputStream1));
        doNothing().when(codec).writeEvent(any(), eq(outputStream2));

        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));


        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            s3SinkService.output(generateEventRecords(2));
        }
        verify(codec).writeEvent(any(), eq(outputStream1));
        verify(codec).writeEvent(any(), eq(outputStream2));
    }

    @Test
    void test_output_with_uploadedToS3_failed() throws IOException {
        when(s3SinkConfig.getBucketName()).thenReturn(UUID.randomUUID().toString());
        when(s3SinkConfig.getMaxUploadRetries()).thenReturn(3);
        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event, outputStream);

        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getOutputCodec()).thenReturn(codec);
        Buffer buffer = mock(Buffer.class);
        when(s3Group.getBuffer()).thenReturn(buffer);

        when(s3GroupManager.getOrCreateGroupForEvent(any(Event.class))).thenReturn(s3Group);
        when(s3GroupManager.getS3GroupEntries()).thenReturn(Collections.singletonList(s3Group));

        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));
        s3SinkService.output(generateLessRandomStringEventRecord());
        verify(snapshotSuccessCounter, times(0)).increment();
    }

    @Test
    void test_output_with_uploadedToS3_failure_does_not_record_byte_count() throws IOException {

        Buffer buffer = mock(Buffer.class);

        final CompletableFuture<?> completableFuture = mock(CompletableFuture.class);
        when(buffer.flushToS3(any(Consumer.class), any(Consumer.class))).thenReturn(Optional.of(completableFuture));

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final S3SinkService s3SinkService = createObjectUnderTest();
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getBuffer()).thenReturn(buffer);
        when(s3Group.getOutputCodec()).thenReturn(codec);

        when(s3GroupManager.getOrCreateGroupForEvent(any(Event.class))).thenReturn(s3Group);
        when(s3GroupManager.getS3GroupEntries()).thenReturn(Collections.emptyList());

        final OutputStream outputStream = mock(OutputStream.class);
        doNothing().when(codec).writeEvent(event, outputStream);


        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            s3SinkService.output(Collections.singletonList(new Record<>(event)));
        }
        final ArgumentCaptor<Consumer<Boolean>> argumentCaptorForCompletion = ArgumentCaptor.forClass(Consumer.class);
        verify(buffer, times(1)).flushToS3(argumentCaptorForCompletion.capture(), any(Consumer.class));

        final Consumer<Boolean> completionConsumer = argumentCaptorForCompletion.getValue();
        completionConsumer.accept(false);

        verify(s3ObjectSizeSummary, never()).record(anyLong());
        verify(s3Group).releaseEventHandles(false);
    }

    @Test
    void test_output_with_no_incoming_records_flushes_batch() throws IOException {

        Buffer buffer = mock(Buffer.class);
        when(buffer.getEventCount()).thenReturn(10);

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());

        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getBuffer()).thenReturn(buffer);
        when(s3Group.getOutputCodec()).thenReturn(codec);

        when(s3GroupManager.getOrCreateGroupForEvent(event)).thenReturn(s3Group);
        when(s3GroupManager.getS3GroupEntries()).thenReturn(Collections.singletonList(s3Group));

        doNothing().when(codec).writeEvent(event, outputStream);
        final S3SinkService s3SinkService = createObjectUnderTest();

        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            s3SinkService.output(Collections.emptyList());
        }
        verify(buffer, times(1)).flushToS3(any(Consumer.class), any(Consumer.class));
    }

    @Test
    void test_output_with_no_incoming_records_or_buffered_records_short_circuits() throws IOException {
        when(s3GroupManager.hasNoGroups()).thenReturn(true);
        final S3SinkService s3SinkService = createObjectUnderTest();
        s3SinkService.output(Collections.emptyList());

        verify(snapshotSuccessCounter, times(0)).increment();
    }

    @Test
    void output_will_release_all_handles_since_a_flush() throws IOException {
        final Buffer buffer = mock(Buffer.class);

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getBuffer()).thenReturn(buffer);
        when(s3Group.getOutputCodec()).thenReturn(codec);

        when(s3GroupManager.getOrCreateGroupForEvent(any(Event.class))).thenReturn(s3Group);
        when(s3GroupManager.getS3GroupEntries()).thenReturn(Collections.singletonList(s3Group));

        doNothing().when(codec).writeEvent(event, outputStream);
        final S3SinkService s3SinkService = createObjectUnderTest();
        final Collection<Record<Event>> records = generateRandomStringEventRecord();
        final List<DefaultEventHandle> eventHandles = records.stream().map(Record::getData).map(Event::getEventHandle).map(this::castToDefaultHandle).collect(Collectors.toList());

        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            s3SinkService.output(records);
        }

        final ArgumentCaptor<Consumer<Boolean>> argumentCaptorForCompletion = ArgumentCaptor.forClass(Consumer.class);
        verify(buffer, times(51)).flushToS3(argumentCaptorForCompletion.capture(), any(Consumer.class));

        final Consumer<Boolean> completionConsumer = argumentCaptorForCompletion.getValue();
        completionConsumer.accept(true);

        InOrder inOrder = inOrder(s3Group);
        for (final EventHandle eventHandle : eventHandles) {
            inOrder.verify(s3Group).addEventHandle(eventHandle);
        }
        inOrder.verify(s3Group).releaseEventHandles(true);

    }

    @Test
    void output_will_skip_releasing_events_without_EventHandle_objects() throws IOException {
        final Buffer buffer = mock(Buffer.class);
        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event1 = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getBuffer()).thenReturn(buffer);
        when(s3Group.getOutputCodec()).thenReturn(codec);

        when(s3GroupManager.getOrCreateGroupForEvent(any(Event.class))).thenReturn(s3Group);

        doNothing().when(codec).writeEvent(event1, outputStream);
        final S3SinkService s3SinkService = createObjectUnderTest();
        final Collection<Record<Event>> records = generateRandomStringEventRecord();
        final List<DefaultEventHandle> eventHandles = records.stream().map(Record::getData).map(Event::getEventHandle).map(this::castToDefaultHandle).collect(Collectors.toList());

        final Collection<Record<Event>> records2 = generateRandomStringEventRecord();
        final List<DefaultEventHandle> eventHandles2 = records2.stream().map(Record::getData).map(Event::getEventHandle).map(this::castToDefaultHandle).collect(Collectors.toList());

        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            s3SinkService.output(records);
            s3SinkService.output(records2);
        }

        final ArgumentCaptor<Consumer<Boolean>> argumentCaptorForCompletion = ArgumentCaptor.forClass(Consumer.class);
        verify(buffer, times(100)).flushToS3(argumentCaptorForCompletion.capture(), any(Consumer.class));

        final Consumer<Boolean> completionConsumer = argumentCaptorForCompletion.getValue();
        completionConsumer.accept(true);

        InOrder inOrder = inOrder(s3Group);
        for (final EventHandle eventHandle : eventHandles) {
            inOrder.verify(s3Group).addEventHandle(eventHandle);
        }
        inOrder.verify(s3Group).releaseEventHandles(true);

    }

    @Test
    void output_will_skip_and_drop_failed_records() throws IOException {
        final Buffer buffer = mock(Buffer.class);

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final OutputStream outputStream = mock(OutputStream.class);
        when(buffer.getOutputStream()).thenReturn(outputStream);


        List<Record<Event>> records = generateEventRecords(2);
        Event event1 = records.get(0).getData();
        Event event2 = records.get(1).getData();
        final S3Group s3Group = mock(S3Group.class);
        when(s3Group.getBuffer()).thenReturn(buffer);
        when(s3Group.getOutputCodec()).thenReturn(codec);

        when(s3GroupManager.getOrCreateGroupForEvent(any(Event.class))).thenReturn(s3Group);
        when(s3GroupManager.getS3GroupEntries()).thenReturn(List.of(s3Group));

        DefaultEventHandle eventHandle1 = (DefaultEventHandle)event1.getEventHandle();
        DefaultEventHandle eventHandle2 = (DefaultEventHandle)event2.getEventHandle();
        eventHandle1.setAcknowledgementSet(acknowledgementSet);
        eventHandle2.setAcknowledgementSet(acknowledgementSet);

        doThrow(RuntimeException.class).when(codec).writeEvent(event1, outputStream);


        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            createObjectUnderTest().output(records);
        }

        InOrder inOrder = inOrder(codec, s3Group);
        inOrder.verify(codec).start(eq(outputStream), eq(event1), any());
        inOrder.verify(codec).writeEvent(event1, outputStream);
        inOrder.verify(s3Group, never()).addEventHandle(eventHandle1);
        inOrder.verify(codec).writeEvent(event2, outputStream);
        inOrder.verify(s3Group).addEventHandle(eventHandle2);

        verify(acknowledgementSet).release(eventHandle1, false);
        verify(acknowledgementSet, never()).release(eventHandle1, true);
        verify(acknowledgementSet, never()).release(eventHandle2, false);
    }

    @Test
    void output_will_flush_the_largest_group_until_below_aggregate_threshold_when_aggregate_threshold_is_reached() throws IOException {
        final long bytesThreshold = 100_000L;
        final long bufferOneSize = 50_000L;
        final long bufferTwoSize = 30_000L;
        final long bufferThreeSize = 70_000L;

        final AggregateThresholdOptions aggregateThresholdOptions = mock(AggregateThresholdOptions.class);
        when(aggregateThresholdOptions.getMaximumSize()).thenReturn(ByteCount.ofBytes(bytesThreshold));
        when(aggregateThresholdOptions.getFlushCapacityRatio()).thenReturn(0.5);
        when(s3SinkConfig.getAggregateThresholdOptions()).thenReturn(aggregateThresholdOptions);
        when(s3GroupManager.recalculateAndGetGroupSize()).thenReturn(bufferOneSize + bufferTwoSize + bufferThreeSize);

        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse("1gb"));


        final Event firstGroupEvent = mock(Event.class);
        final S3Group firstGroup = mock(S3Group.class);
        final Buffer firstGroupBuffer = mock(Buffer.class);
        when(firstGroup.getOutputCodec()).thenReturn(codec);
        when(firstGroupBuffer.getOutputStream()).thenReturn(mock(OutputStream.class));
        when(firstGroupBuffer.getSize()).thenReturn(bufferOneSize);
        when(firstGroup.getBuffer()).thenReturn(firstGroupBuffer);
        when(s3GroupManager.getOrCreateGroupForEvent(firstGroupEvent)).thenReturn(firstGroup);

        final Event secondGroupEvent = mock(Event.class);
        final S3Group secondGroup = mock(S3Group.class);
        final Buffer secondGroupBuffer = mock(Buffer.class);
        when(secondGroup.getOutputCodec()).thenReturn(codec);
        when(secondGroupBuffer.getSize()).thenReturn(bufferTwoSize);
        when(secondGroupBuffer.getOutputStream()).thenReturn(mock(OutputStream.class));
        when(secondGroup.getBuffer()).thenReturn(secondGroupBuffer);
        when(s3GroupManager.getOrCreateGroupForEvent(secondGroupEvent)).thenReturn(secondGroup);

        final Event thirdGroupEvent = mock(Event.class);
        final S3Group thirdGroup = mock(S3Group.class);
        when(thirdGroup.getOutputCodec()).thenReturn(codec);

        final Buffer thirdGroupBuffer = mock(Buffer.class);
        when(thirdGroupBuffer.getSize()).thenReturn(bufferThreeSize);
        when(thirdGroupBuffer.getOutputStream()).thenReturn(mock(OutputStream.class));
        when(thirdGroup.getBuffer()).thenReturn(thirdGroupBuffer);
        when(s3GroupManager.getOrCreateGroupForEvent(thirdGroupEvent)).thenReturn(thirdGroup);

        when(s3GroupManager.getS3GroupEntries()).thenReturn(List.of(firstGroup, secondGroup, thirdGroup));
        when(s3GroupManager.getS3GroupsSortedBySize()).thenReturn(List.of(thirdGroup, firstGroup, secondGroup));

        doNothing().when(codec).start(any(OutputStream.class), any(Event.class), any(OutputCodecContext.class));
        doNothing().when(codec).writeEvent(any(Event.class), any(OutputStream.class));

        final S3SinkService s3SinkService = createObjectUnderTest();

        try (final MockedStatic<CompletableFuture> completableFutureMockedStatic = mockStatic(CompletableFuture.class)) {
            final CompletableFuture<Void> mockCompletableFuture = mock(CompletableFuture.class);
            when(mockCompletableFuture.thenRun(any(Runnable.class))).thenReturn(mockCompletableFuture);
            when(mockCompletableFuture.join()).thenReturn(null);
            completableFutureMockedStatic.when(() -> CompletableFuture.allOf(any())).thenReturn(mockCompletableFuture);
            s3SinkService.output(List.of(new Record<>(firstGroupEvent), new Record<>(secondGroupEvent), new Record<>(thirdGroupEvent)));
        }

        verify(thirdGroupBuffer).flushToS3(any(Consumer.class), any(Consumer.class));
        verify(firstGroupBuffer).flushToS3(any(Consumer.class), any(Consumer.class));

        verify(codec, times(2)).complete(any(OutputStream.class));

        verify(s3GroupManager).removeGroup(thirdGroup);
        verify(s3GroupManager).removeGroup(firstGroup);

        verify(s3GroupManager, never()).removeGroup(secondGroup);
        verify(secondGroupBuffer, never()).flushToS3(any(Consumer.class), any(Consumer.class));

        verify(s3ObjectsForceFlushedCounter, times(2)).increment();
    }

    private Collection<Record<Event>> generateRandomStringEventRecord() {
        return generateEventRecords(50);
    }

    private Collection<Record<Event>> generateLessRandomStringEventRecord() {
        return generateEventRecords(5);
    }

    private List<Record<Event>> generateEventRecords(final int numberOfRecords) {
        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            final JacksonEvent event = (JacksonEvent) JacksonEvent.fromMessage(UUID.randomUUID().toString());
            records.add(new Record<>(event));
        }
        return records;
    }

    private byte[] generateByteArray() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}
