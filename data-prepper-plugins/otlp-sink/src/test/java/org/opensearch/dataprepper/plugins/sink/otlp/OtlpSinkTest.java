/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.apache.commons.codec.DecoderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender;
import software.amazon.awssdk.regions.Region;

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OtlpSinkTest {

    private OtlpSinkConfig mockConfig;
    private OTelProtoStandardCodec.OTelProtoEncoder mockEncoder;
    private OtlpHttpSender mockSender;
    private PluginMetrics mockPluginMetrics;
    private OtlpSink target;

    @BeforeEach
    void setUp() {
        System.setProperty("aws.accessKeyId", "dummy");
        System.setProperty("aws.secretAccessKey", "dummy");

        mockConfig = mock(OtlpSinkConfig.class);
        when(mockConfig.getAwsRegion()).thenReturn(Region.US_WEST_2);
        when(mockConfig.getBatchSize()).thenReturn(100);
        when(mockConfig.getMaxRetries()).thenReturn(3);

        mockEncoder = mock(OTelProtoStandardCodec.OTelProtoEncoder.class);
        mockSender = mock(OtlpHttpSender.class);

        mockPluginMetrics = mock(PluginMetrics.class);
        mockPluginMetrics = mock(PluginMetrics.class);
        when(mockPluginMetrics.counter(anyString())).thenReturn(mock(Counter.class));
        when(mockPluginMetrics.summary(anyString())).thenReturn(mock(DistributionSummary.class));

        target = new OtlpSink(mockConfig, mockPluginMetrics, mockEncoder, mockSender);
    }

    @AfterEach
    void cleanUp() {
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
        System.clearProperty("aws.region");
    }

    @Test
    void testOutput_shouldSendAllBatches() throws Exception {
        final int recordCount = 250;
        final List<Record<Span>> records = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            Record<Span> mockRecord = mock(Record.class);
            Span span = mock(Span.class);
            ResourceSpans resourceSpans = ResourceSpans.getDefaultInstance();
            when(mockEncoder.convertToResourceSpans(span)).thenReturn(resourceSpans);
            when(mockRecord.getData()).thenReturn(span);
            records.add(mockRecord);
        }

        target.output(records);

        // 250 total / 100 batch size = 3 calls to httpSender
        verify(mockSender, times(3)).send(any(byte[].class));
        verify(mockEncoder, times(recordCount)).convertToResourceSpans(any());
    }

    @Test
    void testOutput_shouldHandleRuntimeException() throws Exception {
        Span span = mock(Span.class);
        when(span.getSpanId()).thenReturn("span123");
        Record<Span> record = mock(Record.class);
        when(record.getData()).thenReturn(span);

        when(mockEncoder.convertToResourceSpans(span)).thenReturn(ResourceSpans.getDefaultInstance());
        doThrow(new RuntimeException("sender failure")).when(mockSender).send(any());

        target.output(List.of(record));

        verify(mockEncoder).convertToResourceSpans(eq(span));
        verify(mockSender).send(any());
    }

    @Test
    void testOutput_shouldSendPartialBatchWhenSomeSpansSucceed() throws Exception {
        // Good span
        Span goodSpan = mock(Span.class);
        when(goodSpan.getSpanId()).thenReturn("good-span");
        Record<Span> goodRecord = mock(Record.class);
        when(goodRecord.getData()).thenReturn(goodSpan);

        // Bad span (encoder throws)
        Span badSpan = mock(Span.class);
        when(badSpan.getSpanId()).thenReturn("bad-span");
        Record<Span> badRecord = mock(Record.class);
        when(badRecord.getData()).thenReturn(badSpan);

        // Good span gets encoded properly
        ResourceSpans goodResourceSpans = ResourceSpans.getDefaultInstance();
        when(mockEncoder.convertToResourceSpans(goodSpan)).thenReturn(goodResourceSpans);

        // Bad span causes exception during encode
        when(mockEncoder.convertToResourceSpans(badSpan)).thenThrow(new RuntimeException("bad span"));

        target.output(List.of(badRecord, goodRecord));

        // Encoder is called on both
        verify(mockEncoder).convertToResourceSpans(badSpan);
        verify(mockEncoder).convertToResourceSpans(goodSpan);

        // Sender should still be called with only the good span
        verify(mockSender, times(1)).send(any(byte[].class));
    }

    @Test
    void testOutput_shouldNotSendEmptyBatch() throws DecoderException, UnsupportedEncodingException {
        Span span = mock(Span.class);
        when(span.getSpanId()).thenReturn("bad-span");
        Record<Span> record = mock(Record.class);
        when(record.getData()).thenReturn(span);

        when(mockEncoder.convertToResourceSpans(span)).thenReturn(null); // simulate invalid span

        target.output(List.of(record));

        verify(mockSender, never()).send(any());
    }

    @Test
    void testUpdateLatencyMetrics_shouldRecordLatency() {
        Span mockSpan = mock(Span.class);
        String startTime = Instant.now().minusSeconds(5).toString();
        when(mockSpan.getStartTime()).thenReturn(startTime);

        Record<Span> mockRecord = mock(Record.class);
        when(mockRecord.getData()).thenReturn(mockSpan);

        Collection<Record<Span>> events = List.of(mockRecord);

        target.updateLatencyMetrics(events);

        verify(mockPluginMetrics.summary("deliveryLatency"), times(1)).record(any(Double.class));
    }

    @Test
    void testUpdateLatencyMetrics_shouldHandleInvalidStartTime() {
        // Mock a Span with an invalid start time string
        final Span badSpan = mock(Span.class);
        when(badSpan.getStartTime()).thenReturn("invalid-timestamp");

        final Record<Span> badRecord = mock(Record.class);
        when(badRecord.getData()).thenReturn(badSpan);

        final List<Record<Span>> records = List.of(badRecord);

        target.updateLatencyMetrics(records);

        // Ensure it attempted to parse and failed gracefully
        verify(mockPluginMetrics.summary("deliveryLatency"), never()).record(anyDouble());
        verify(mockPluginMetrics.counter("errorsCount")).increment(1);
    }

    @Test
    void testConstructor_withOnlyConfig_shouldInitializeWithoutException() {
        OtlpSink sink = new OtlpSink(mockConfig, mockPluginMetrics);

        sink.initialize();
        sink.shutdown();
    }

    @Test
    void testIsReady_returnsTrue() {
        assert target.isReady();
    }

    @Test
    void testShutdown_doesNotThrow() {
        target.shutdown();
    }
}
