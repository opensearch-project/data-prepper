/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.csv;

import com.amazon.dataprepper.metrics.PluginMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;

public class CSVProcessorTest {
    @Mock
    private CSVProcessorConfig processorConfig;

    @Mock
    private PluginMetrics pluginMetrics;
    @BeforeEach
    void setup() {

    }

    private CSVProcessor createObjectUnderTest() {
        return new CSVProcessor(
                pluginMetrics, processorConfig
                );
    }
}
