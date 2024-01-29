/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.DecoderEngine;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.IEncodingType;
import org.opensearch.dataprepper.plugins.processor.decompress.exceptions.DecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.decompress.DecompressProcessor.DECOMPRESSION_PROCESSING_ERRORS;

@ExtendWith(MockitoExtension.class)
public class DecompressProcessorTest {

    private String key;

    @Mock
    private DecompressionEngine decompressionEngine;

    @Mock
    private DecoderEngine decoderEngine;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter decompressionProcessingErrors;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private DecompressProcessorConfig decompressProcessorConfig;

    @Mock
    private IDecompressionType decompressionType;

    @Mock
    private IEncodingType encodingType;

    private DecompressProcessor createObjectUnderTest() {
        return new DecompressProcessor(pluginMetrics, decompressProcessorConfig, expressionEvaluator);
    }

    @BeforeEach
    void setup() {
        key = UUID.randomUUID().toString();

        when(pluginMetrics.counter(MetricNames.RECORDS_IN)).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter(MetricNames.RECORDS_OUT)).thenReturn(mock(Counter.class));
        when(pluginMetrics.timer(MetricNames.TIME_ELAPSED)).thenReturn(mock(Timer.class));
        when(pluginMetrics.counter(DECOMPRESSION_PROCESSING_ERRORS)).thenReturn(decompressionProcessingErrors);
    }

    @Test
    void decompression_returns_expected_output() throws IOException {
        final String compressedValue = UUID.randomUUID().toString();
        final String expectedResult = UUID.randomUUID().toString();
        final byte[] decodedValue = expectedResult.getBytes();

        when(decompressProcessorConfig.getKeys()).thenReturn(List.of(key));
        when(encodingType.getDecoderEngine()).thenReturn(decoderEngine);
        when(decompressProcessorConfig.getEncodingType()).thenReturn(encodingType);
        when(decompressProcessorConfig.getDecompressionType()).thenReturn(decompressionType);
        when(decompressionType.getDecompressionEngine()).thenReturn(decompressionEngine);
        when(decoderEngine.decode(compressedValue)).thenReturn(decodedValue);
        when(decompressionEngine.createInputStream(any(InputStream.class))).thenReturn(new ByteArrayInputStream(decodedValue));

        final List<Record<Event>> records = List.of(buildRecordWithEvent(Map.of(key, compressedValue)));

        final DecompressProcessor objectUnderTest = createObjectUnderTest();
        final List<Record<Event>> result = (List<Record<Event>>) objectUnderTest.doExecute(records);

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), notNullValue());
        assertThat(result.get(0).getData(), notNullValue());
        assertThat(result.get(0).getData().get(key, String.class), equalTo(expectedResult));
    }

    @Test
    void decompression_with_decoding_error_adds_tags_and_increments_error_metric() {
        final String compressedValue = UUID.randomUUID().toString();
        final String tagForFailure = UUID.randomUUID().toString();

        when(decompressProcessorConfig.getKeys()).thenReturn(List.of(key));
        when(encodingType.getDecoderEngine()).thenReturn(decoderEngine);
        when(decompressProcessorConfig.getEncodingType()).thenReturn(encodingType);
        when(decompressProcessorConfig.getTagsOnFailure()).thenReturn(List.of(tagForFailure));
        when(decoderEngine.decode(compressedValue)).thenThrow(DecodingException.class);

        final List<Record<Event>> records = List.of(buildRecordWithEvent(Map.of(key, compressedValue)));

        final DecompressProcessor objectUnderTest = createObjectUnderTest();
        final List<Record<Event>> result = (List<Record<Event>>) objectUnderTest.doExecute(records);

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), notNullValue());
        assertThat(result.get(0).getData(), notNullValue());
        assertThat(result.get(0).getData().get(key, String.class), equalTo(compressedValue));
        assertThat(result.get(0).getData().getMetadata().getTags(), notNullValue());
        assertThat(result.get(0).getData().getMetadata().getTags().size(), equalTo(1));
        assertThat(result.get(0).getData().getMetadata().getTags().contains(tagForFailure), equalTo(true));

        verifyNoInteractions(decompressionEngine);
        verify(decompressionProcessingErrors).increment();
    }

    @Test
    void exception_from_DecompressionEngine_adds_tags_and_increments_error_metric() throws IOException {
        final String compressedValue = UUID.randomUUID().toString();
        final String expectedResult = UUID.randomUUID().toString();
        final byte[] decodedValue = expectedResult.getBytes();
        final String tagForFailure = UUID.randomUUID().toString();

        when(decompressProcessorConfig.getKeys()).thenReturn(List.of(key));
        when(encodingType.getDecoderEngine()).thenReturn(decoderEngine);
        when(decompressProcessorConfig.getEncodingType()).thenReturn(encodingType);
        when(decompressProcessorConfig.getTagsOnFailure()).thenReturn(List.of(tagForFailure));
        when(decompressProcessorConfig.getDecompressionType()).thenReturn(decompressionType);
        when(decompressionType.getDecompressionEngine()).thenReturn(decompressionEngine);
        when(decoderEngine.decode(compressedValue)).thenReturn(decodedValue);
        when(decompressionEngine.createInputStream(any(InputStream.class))).thenThrow(RuntimeException.class);

        final List<Record<Event>> records = List.of(buildRecordWithEvent(Map.of(key, compressedValue)));

        final DecompressProcessor objectUnderTest = createObjectUnderTest();
        final List<Record<Event>> result = (List<Record<Event>>) objectUnderTest.doExecute(records);

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), notNullValue());
        assertThat(result.get(0).getData(), notNullValue());
        assertThat(result.get(0).getData().get(key, String.class), equalTo(compressedValue));
        assertThat(result.get(0).getData().getMetadata().getTags(), notNullValue());
        assertThat(result.get(0).getData().getMetadata().getTags().size(), equalTo(1));
        assertThat(result.get(0).getData().getMetadata().getTags().contains(tagForFailure), equalTo(true));

        verify(decompressionProcessingErrors).increment();
    }

    @Test
    void exception_from_expression_evaluator_adds_tags_and_increments_error_metric() {
        final String decompressWhen = UUID.randomUUID().toString();
        final String compressedValue = UUID.randomUUID().toString();
        final String tagForFailure = UUID.randomUUID().toString();

        when(decompressProcessorConfig.getTagsOnFailure()).thenReturn(List.of(tagForFailure));
        when(decompressProcessorConfig.getDecompressWhen()).thenReturn(decompressWhen);
        when(expressionEvaluator.evaluateConditional(eq(decompressWhen), any(Event.class)))
                .thenThrow(RuntimeException.class);

        final List<Record<Event>> records = List.of(buildRecordWithEvent(Map.of(key, compressedValue)));

        final DecompressProcessor objectUnderTest = createObjectUnderTest();
        final List<Record<Event>> result = (List<Record<Event>>) objectUnderTest.doExecute(records);

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), notNullValue());
        assertThat(result.get(0).getData(), notNullValue());
        assertThat(result.get(0).getData().get(key, String.class), equalTo(compressedValue));
        assertThat(result.get(0).getData().getMetadata().getTags(), notNullValue());
        assertThat(result.get(0).getData().getMetadata().getTags().size(), equalTo(1));
        assertThat(result.get(0).getData().getMetadata().getTags().contains(tagForFailure), equalTo(true));

        verifyNoInteractions(decoderEngine, decompressionEngine);
        verify(decompressionProcessingErrors).increment();
    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
