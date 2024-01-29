/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.decompress.encoding.EncodingType;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.decompress.DecompressProcessorTest.buildRecordWithEvent;

@ExtendWith(MockitoExtension.class)
public class ITDecompressProcessorTest {

    private List<String> keys;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private DecompressProcessorConfig decompressProcessorConfig;

    private DecompressProcessor createObjectUnderTest() {
        return new DecompressProcessor(pluginMetrics, decompressProcessorConfig, expressionEvaluator);
    }

    @BeforeEach
    void setup() {
        keys = List.of(UUID.randomUUID().toString());
        when(decompressProcessorConfig.getKeys()).thenReturn(keys);
    }

    @ParameterizedTest
    @CsvSource({"H4sIAAAAAAAAAPNIzcnJVyjPL8pJAQBSntaLCwAAAA==,Hello world",
                "H4sIAAAAAAAAAwvJyCxWAKJEhYKcxMy8ktSKEoXikqLMvHQAkJ3GfRoAAAA=,This is a plaintext string"})
    void base64_encoded_gzip_is_decompressed_successfully(final String compressedValue, final String expectedDecompressedValue) {
        when(decompressProcessorConfig.getEncodingType()).thenReturn(EncodingType.BASE64);
        when(decompressProcessorConfig.getDecompressionType()).thenReturn(DecompressionType.GZIP);

        final DecompressProcessor objectUnderTest = createObjectUnderTest();
        final List<Record<Event>> records = List.of(buildRecordWithEvent(Map.of(keys.get(0), compressedValue)));

        final List<Record<Event>> result = (List<Record<Event>>) objectUnderTest.doExecute(records);

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), notNullValue());
        assertThat(result.get(0).getData(), notNullValue());
        assertThat(result.get(0).getData().get(keys.get(0), String.class), equalTo(expectedDecompressedValue));
    }
}
