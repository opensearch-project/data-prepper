/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TimeZone;
import java.util.UUID;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.sink.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.codec.Codec;
import org.opensearch.dataprepper.plugins.sink.codec.JsonCodec;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

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
    private AwsCredentialsProvider awsCredentialsProvider;
    private JsonCodec codec;
    private PluginMetrics pluginMetrics;
    private BufferFactory bufferFactory;
    private Counter snapshotSuccessCounter;

    @BeforeEach
    void setUp() throws Exception {

        s3SinkConfig = mock(S3SinkConfig.class);
        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
        BucketOptions bucketOptions = mock(BucketOptions.class);
        ObjectKeyOptions objectKeyOptions = mock(ObjectKeyOptions.class);
        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        PluginSetting pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        PluginModel pluginModel = mock(PluginModel.class);
        PluginFactory pluginFactory = mock(PluginFactory.class);
        codec = mock(JsonCodec.class);
        snapshotSuccessCounter = mock(Counter.class);
        Counter snapshotFailedCounter = mock(Counter.class);
        Counter numberOfRecordsSuccessCounter = mock(Counter.class);
        Counter numberOfRecordsFailedCounter = mock(Counter.class);
        DistributionSummary s3ObjectSizeSummary = mock(DistributionSummary.class);

        bufferFactory = new InMemoryBufferFactory();

        when(objectKeyOptions.getNamePattern()).thenReturn(OBJECT_KEY_NAME_PATTERN);
        when(s3SinkConfig.getMaxUploadRetries()).thenReturn(MAX_RETRIES);
        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);
        when(s3SinkConfig.getThresholdOptions().getEventCount()).thenReturn(MAX_EVENTS);
        when(s3SinkConfig.getThresholdOptions().getMaximumSize()).thenReturn(ByteCount.parse(MAXIMUM_SIZE));
        when(s3SinkConfig.getThresholdOptions().getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(5));
        when(s3SinkConfig.getBufferType()).thenReturn(BufferTypeOptions.INMEMORY);
        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(s3SinkConfig.getBucketOptions().getBucketName()).thenReturn(BUCKET_NAME);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getPathPrefix()).thenReturn(PATH_PREFIX);
        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of(S3_REGION));
        when(awsAuthenticationOptions.authenticateAwsConfiguration()).thenReturn(awsCredentialsProvider);
        when(s3SinkConfig.getCodec()).thenReturn(pluginModel);
        when(pluginModel.getPluginName()).thenReturn(CODEC_PLUGIN_NAME);
        when(pluginFactory.loadPlugin(Codec.class, pluginSetting)).thenReturn(codec);

        lenient().when(pluginMetrics.counter(S3SinkService.SNAPSHOT_SUCCESS)).thenReturn(snapshotSuccessCounter);
        lenient().when(pluginMetrics.counter(S3SinkService.SNAPSHOT_FAILED)).thenReturn(snapshotFailedCounter);
        lenient().when(pluginMetrics.counter(S3SinkService.NUMBER_OF_RECORDS_FLUSHED_TO_S3_SUCCESS)).
                thenReturn(numberOfRecordsSuccessCounter);
        lenient().when(pluginMetrics.counter(S3SinkService.NUMBER_OF_RECORDS_FLUSHED_TO_S3_FAILED)).
                thenReturn(numberOfRecordsFailedCounter);
        lenient().when(pluginMetrics.summary(S3SinkService.S3_OBJECTS_SIZE)).thenReturn(s3ObjectSizeSummary);
    }

    @Test
    void test_s3SinkService_notNull() {
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec, pluginMetrics);
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));
    }

    @Test
    void test_s3Client_notNull() {
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec, pluginMetrics);
        S3Client s3Client = s3SinkService.createS3Client();
        assertNotNull(s3Client);
        assertThat(s3Client, instanceOf(S3Client.class));
    }

    @Test
    void test_generateKey_with_general_prefix() {
        String pathPrefix = "events/";
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getPathPrefix()).thenReturn(pathPrefix);
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec, pluginMetrics);
        String key = s3SinkService.generateKey();
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

        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()
                .getPathPrefix()).thenReturn(pathPrefix + datePattern);
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec, pluginMetrics);
        String key = s3SinkService.generateKey();
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
        when(codec.parse(any())).thenReturn("{\"message\":\"31824252-adba-4c47-a2ac-05d16c5b8140\"}");
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec, pluginMetrics);
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
        when(codec.parse(any())).thenReturn("{\"message\":\"31824252-adba-4c47-a2ac-05d16c5b8140\"}");
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec, pluginMetrics);
        assertNotNull(s3SinkService);
        s3SinkService.output(generateRandomStringEventRecord());
        verify(snapshotSuccessCounter, times(50)).increment();
    }

    @Test
    void test_catch_output_exception_cover() {
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, null, pluginMetrics);
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));
        s3SinkService.output(generateRandomStringEventRecord());
        verify(snapshotSuccessCounter, times(0)).increment();
    }

    @Test
    void test_output_with_uploadedToS3_success() throws IOException {

        bufferFactory = mock(BufferFactory.class);
        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        when(buffer.getEventCount()).thenReturn(10);
        doNothing().when(buffer).flushToS3(any(S3Client.class), anyString(), any(String.class));
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        when(codec.parse(any())).thenReturn("{\"message\":\"31824252-adba-4c47-a2ac-05d16c5b8140\"}");
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec, pluginMetrics);
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));
        s3SinkService.output(generateRandomStringEventRecord());
        verify(snapshotSuccessCounter, times(50)).increment();
    }

    @Test
    void test_output_with_uploadedToS3_failed() throws IOException {
        when(s3SinkConfig.getBucketOptions().getBucketName()).thenReturn(null);
        when(s3SinkConfig.getMaxUploadRetries()).thenReturn(3);
        when(codec.parse(any())).thenReturn("{\"message\":\"31824252-adba-4c47-a2ac-05d16c5b8140\"}");
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec, pluginMetrics);
        assertNotNull(s3SinkService);
        assertThat(s3SinkService, instanceOf(S3SinkService.class));
        s3SinkService.output(generateLessRandomStringEventRecord());
        verify(snapshotSuccessCounter, times(0)).increment();
    }

    @Test
    void test_retryFlushToS3_positive() throws InterruptedException, IOException {

        bufferFactory = mock(BufferFactory.class);
        InMemoryBuffer buffer = mock(InMemoryBuffer.class);
        doNothing().when(buffer).flushToS3(any(S3Client.class), anyString(), any(String.class));
        when(bufferFactory.getBuffer()).thenReturn(buffer);

        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec, pluginMetrics);
        assertNotNull(s3SinkService);
        assertNotNull(buffer);
        buffer.writeEvent(generateByteArray());
        boolean isUploadedToS3 = s3SinkService.retryFlushToS3(buffer);
        assertTrue(isUploadedToS3);
    }

    @Test
    void test_retryFlushToS3_negative() throws InterruptedException, IOException {
        when(s3SinkConfig.getBucketOptions().getBucketName()).thenReturn("");
        S3SinkService s3SinkService = new S3SinkService(s3SinkConfig, bufferFactory, codec, pluginMetrics);
        assertNotNull(s3SinkService);
        Buffer buffer = bufferFactory.getBuffer();
        assertNotNull(buffer);
        buffer.writeEvent(generateByteArray());
        boolean isUploadedToS3 = s3SinkService.retryFlushToS3(buffer);
        assertFalse(isUploadedToS3);
    }

    private Collection<Record<Event>> generateRandomStringEventRecord() {
        Collection<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
            records.add(new Record<>(event));
        }
        return records;
    }

    private Collection<Record<Event>> generateLessRandomStringEventRecord() {
        Collection<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
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