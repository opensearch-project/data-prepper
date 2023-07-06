/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    private PluginMetrics pluginMetrics;
    private BufferFactory bufferFactory;
    private Counter snapshotSuccessCounter;
    private DistributionSummary s3ObjectSizeSummary;
    private Random random;
    private String tagsTargetKey;

    @BeforeEach
    void setUp() {

        random = new Random();
        tagsTargetKey = RandomStringUtils.randomAlphabetic(5);
        s3SinkConfig = mock(S3SinkConfig.class);
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
        Counter snapshotFailedCounter = mock(Counter.class);
        Counter numberOfRecordsSuccessCounter = mock(Counter.class);
        Counter numberOfRecordsFailedCounter = mock(Counter.class);
        s3ObjectSizeSummary = mock(DistributionSummary.class);

        bufferFactory = new InMemoryBufferFactory();

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
    }

    private S3SinkService createObjectUnderTest() {
        return new S3SinkService(s3SinkConfig, bufferFactory, codec, s3Client, tagsTargetKey, pluginMetrics);
    }

    @Test
    void test_s3SinkService_notNull() {
        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));
    }

    @Test
    void test_generateKey_with_general_prefix() {
        String pathPrefix = "events/";
        when(s3SinkConfig.getObjectKeyOptions().getPathPrefix()).thenReturn(pathPrefix);
        S3SinkService s3SinkService = createObjectUnderTest();
        String key = s3SinkService.generateKey(codec);
        assertNotNull(key);
        assertThat(key, true);
        assertThat(key, key.contains(pathPrefix));
    }

    @Test
    void test_generateKey_with_date_prefix() {
        String pathPrefix = "logdata/";
        String datePattern = "%{yyyy}/%{MM}/%{dd}/";

        DateTimeFormatter fomatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
        ZonedDateTime zdt = LocalDateTime.now().atZone(ZoneId.systemDefault())
                .withZoneSameInstant(ZoneId.of(TimeZone.getTimeZone("UTC").getID()));
        String dateString = fomatter.format(zdt);

        when(s3SinkConfig.getObjectKeyOptions()
                .getPathPrefix()).thenReturn(pathPrefix + datePattern);
        S3SinkService s3SinkService = createObjectUnderTest();
        String key = s3SinkService.generateKey(codec);
        assertNotNull(key);
        assertThat(key, true);
        assertThat(key, key.contains(pathPrefix + dateString));
    }

    @Test
    void test_output_with_threshold_set_as_more_then_zero_event_count() throws IOException {
        bufferFactory = mock(BufferFactory.class);
        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        when(buffer.getEventCount()).thenReturn(10);
        doNothing().when(buffer).flushToS3(any(S3Client.class), anyString(), any(String.class));
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(5);
        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event, outputStream, null);
        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        s3SinkService.output(generateRandomStringEventRecord());
        verify(snapshotSuccessCounter, times(50)).increment();
    }


    // If event_count threshold set as zero, Hence event_count will be
    // ignored as part of threshold check.
    @Test
    void test_output_with_threshold_set_as_zero_event_count() throws IOException {

        bufferFactory = mock(BufferFactory.class);
        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        when(buffer.getSize()).thenReturn(25500L);
        doNothing().when(buffer).flushToS3(any(S3Client.class), anyString(), any(String.class));
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(0);
        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse("2kb"));
        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event, outputStream, null);
        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        s3SinkService.output(generateRandomStringEventRecord());
        verify(snapshotSuccessCounter, times(50)).increment();
    }

    @Test
    void test_output_with_uploadedToS3_success() throws IOException {

        bufferFactory = mock(BufferFactory.class);
        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        when(buffer.getEventCount()).thenReturn(10);
        doNothing().when(buffer).flushToS3(any(S3Client.class), anyString(), any(String.class));
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event, outputStream, null);
        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));
        s3SinkService.output(generateRandomStringEventRecord());
        verify(snapshotSuccessCounter, times(50)).increment();
    }

    @Test
    void test_output_with_uploadedToS3_success_records_byte_count() throws IOException {

        bufferFactory = mock(BufferFactory.class);
        Buffer buffer = mock(Buffer.class);
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event, outputStream, null);
        final S3SinkService s3SinkService = createObjectUnderTest();
        s3SinkService.output(generateRandomStringEventRecord());

        verify(s3ObjectSizeSummary, times(50)).record(objectSize);
    }

    @Test
    void test_output_with_uploadedToS3_failed() throws IOException {
        when(s3SinkConfig.getBucketName()).thenReturn(UUID.randomUUID().toString());
        when(s3SinkConfig.getMaxUploadRetries()).thenReturn(3);
        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event, outputStream, null);
        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));
        s3SinkService.output(generateLessRandomStringEventRecord());
        verify(snapshotSuccessCounter, times(0)).increment();
    }

    @Test
    void test_output_with_uploadedToS3_failure_does_not_record_byte_count() throws IOException {

        bufferFactory = mock(BufferFactory.class);
        Buffer buffer = mock(Buffer.class);
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        doThrow(AwsServiceException.class).when(buffer).flushToS3(any(), anyString(), anyString());

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final S3SinkService s3SinkService = createObjectUnderTest();
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        final OutputStream outputStream = mock(OutputStream.class);
        doNothing().when(codec).writeEvent(event, outputStream, null);
        s3SinkService.output(Collections.singletonList(new Record<>(event)));

        verify(s3ObjectSizeSummary, never()).record(anyLong());
        verify(buffer, times(3)).flushToS3(any(), anyString(), anyString());
    }

    @Test
    void test_retryFlushToS3_positive() throws InterruptedException, IOException {

        bufferFactory = mock(BufferFactory.class);
        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        doNothing().when(buffer).flushToS3(any(S3Client.class), anyString(), any(String.class));
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        assertNotNull(buffer);
        OutputStream outputStream = buffer.getOutputStream();
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        codec.writeEvent(event, outputStream, null);
        final String s3Key = UUID.randomUUID().toString();
        boolean isUploadedToS3 = s3SinkService.retryFlushToS3(buffer, s3Key);
        assertTrue(isUploadedToS3);
    }

    @Test
    void test_retryFlushToS3_negative() throws InterruptedException, IOException {
        bufferFactory = mock(BufferFactory.class);
        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        when(bufferFactory.getBuffer()).thenReturn(buffer);
        when(s3SinkConfig.getBucketName()).thenReturn("");
        S3SinkService s3SinkService = createObjectUnderTest();
        assertNotNull(s3SinkService);
        OutputStream outputStream = buffer.getOutputStream();
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        codec.writeEvent(event, outputStream, null);
        final String s3Key = UUID.randomUUID().toString();
        doThrow(AwsServiceException.class).when(buffer).flushToS3(eq(s3Client), anyString(), anyString());
        boolean isUploadedToS3 = s3SinkService.retryFlushToS3(buffer, s3Key);
        assertFalse(isUploadedToS3);
    }


    @Test
    void output_will_release_all_handles_since_a_flush() throws IOException {
        bufferFactory = mock(BufferFactory.class);
        final Buffer buffer = mock(Buffer.class);
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event, outputStream, null);
        final S3SinkService s3SinkService = createObjectUnderTest();
        final Collection<Record<Event>> records = generateRandomStringEventRecord();
        s3SinkService.output(records);

        final List<EventHandle> eventHandles = records.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        for (EventHandle eventHandle : eventHandles) {
            verify(eventHandle).release(true);
        }
    }

    @Test
    void output_will_skip_releasing_events_without_EventHandle_objects() throws IOException {
        bufferFactory = mock(BufferFactory.class);
        final Buffer buffer = mock(Buffer.class);
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event1 = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event1, outputStream, null);
        final S3SinkService s3SinkService = createObjectUnderTest();
        final Collection<Record<Event>> records = generateRandomStringEventRecord();
        records.stream()
                .map(Record::getData)
                .map(event -> (JacksonEvent) event)
                .forEach(event -> event.setEventHandle(null));

        s3SinkService.output(records);

        final Collection<Record<Event>> records2 = generateRandomStringEventRecord();
        s3SinkService.output(records2);

        final List<EventHandle> eventHandles2 = records2.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        for (EventHandle eventHandle : eventHandles2) {
            verify(eventHandle).release(true);
        }
    }

    @Test
    void output_will_release_all_handles_since_a_flush_when_S3_fails() throws IOException {
        bufferFactory = mock(BufferFactory.class);
        final Buffer buffer = mock(Buffer.class);
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        doThrow(AwsServiceException.class).when(buffer).flushToS3(any(), anyString(), anyString());

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event, outputStream, null);
        final S3SinkService s3SinkService = createObjectUnderTest();
        final List<Record<Event>> records = generateEventRecords(1);
        s3SinkService.output(records);

        final List<EventHandle> eventHandles = records.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        for (EventHandle eventHandle : eventHandles) {
            verify(eventHandle).release(false);
        }
    }

    @Test
    void output_will_release_only_new_handles_since_a_flush() throws IOException {
        bufferFactory = mock(BufferFactory.class);
        final Buffer buffer = mock(Buffer.class);
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event, outputStream, null);
        final S3SinkService s3SinkService = createObjectUnderTest();
        final Collection<Record<Event>> records = generateRandomStringEventRecord();
        s3SinkService.output(records);
        final Collection<Record<Event>> records2 = generateRandomStringEventRecord();
        s3SinkService.output(records2);

        final List<EventHandle> eventHandles1 = records.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        for (EventHandle eventHandle : eventHandles1) {
            verify(eventHandle).release(true);
        }

        final List<EventHandle> eventHandles2 = records2.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        for (EventHandle eventHandle : eventHandles2) {
            verify(eventHandle).release(true);
        }
    }

    @Test
    void output_will_release_only_new_handles_since_a_flush_when_S3_fails() throws IOException {
        bufferFactory = mock(BufferFactory.class);
        final Buffer buffer = mock(Buffer.class);
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        doThrow(AwsServiceException.class).when(buffer).flushToS3(any(), anyString(), anyString());

        final long objectSize = random.nextInt(1_000_000) + 10_000;
        when(buffer.getSize()).thenReturn(objectSize);

        final OutputStream outputStream = mock(OutputStream.class);
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        doNothing().when(codec).writeEvent(event, outputStream, null);
        final S3SinkService s3SinkService = createObjectUnderTest();
        final List<Record<Event>> records = generateEventRecords(1);
        s3SinkService.output(records);
        final List<Record<Event>> records2 = generateEventRecords(1);
        s3SinkService.output(records2);

        final List<EventHandle> eventHandles = records.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        for (EventHandle eventHandle : eventHandles) {
            verify(eventHandle).release(false);
        }
        final List<EventHandle> eventHandles2 = records2.stream().map(Record::getData).map(Event::getEventHandle).collect(Collectors.toList());

        for (EventHandle eventHandle : eventHandles2) {
            verify(eventHandle).release(false);
        }
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
            final EventHandle eventHandle = mock(EventHandle.class);
            event.setEventHandle(eventHandle);
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