/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.csv;

import com.amazon.dataprepper.metrics.PluginMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class CSVProcessorTest {

    @Mock
    private CSVProcessorConfig processorConfig;

    @Mock
    private PluginMetrics pluginMetrics;
    @BeforeEach
    void setup() {

    }

    private CSVProcessor createObjectUnderTest() {
        return new CSVProcessor(pluginMetrics, processorConfig);
    }

    @Test
    public void test_when_CSVProcessor_created_then_throws_NotImplemented_exception() {
        assertThrows(UnsupportedOperationException.class, () -> createObjectUnderTest());
    }
}