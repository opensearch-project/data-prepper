/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.TestDataProvider;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PipelineConfigurationTests {

    @Test
    public void testPipelineConfigurationCreation() {
        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(TestDataProvider.validSingleConfiguration(),
                null,
                TestDataProvider.validMultipleConfigurationOfSizeOne(),
                TestDataProvider.validMultipleConfiguration(),
                TestDataProvider.TEST_WORKERS, TestDataProvider.TEST_DELAY);
        final PluginSetting actualSourcePluginSetting = pipelineConfiguration.getSourcePluginSetting();
        final PluginSetting actualBufferPluginSetting = pipelineConfiguration.getBufferPluginSetting();
        final List<PluginSetting> actualProcesserPluginSettings = pipelineConfiguration.getProcessorPluginSettings();
        final List<PluginSetting> actualSinkPluginSettings = pipelineConfiguration.getSinkPluginSettings();

        comparePluginSettings(actualSourcePluginSetting, TestDataProvider.VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getBufferPluginSetting(), notNullValue());
        comparePluginSettings(actualBufferPluginSetting, BlockingBuffer.getDefaultPluginSettings());
        assertThat(actualProcesserPluginSettings.size(), is(1));
        actualProcesserPluginSettings.forEach(processorSettings -> comparePluginSettings(processorSettings, TestDataProvider.VALID_PLUGIN_SETTING_1));
        assertThat(actualSinkPluginSettings.size(), is(2));
        comparePluginSettings(actualSinkPluginSettings.get(0), TestDataProvider.VALID_PLUGIN_SETTING_1);
        comparePluginSettings(actualSinkPluginSettings.get(1), TestDataProvider.VALID_PLUGIN_SETTING_2);
        assertThat(pipelineConfiguration.getWorkers(), CoreMatchers.is(TestDataProvider.TEST_WORKERS));
        assertThat(pipelineConfiguration.getReadBatchDelay(), CoreMatchers.is(TestDataProvider.TEST_DELAY));

        pipelineConfiguration.updateCommonPipelineConfiguration(TestDataProvider.TEST_PIPELINE_NAME);
        assertThat(actualSourcePluginSetting.getPipelineName(), is(equalTo(TestDataProvider.TEST_PIPELINE_NAME)));
        assertThat(actualSourcePluginSetting.getNumberOfProcessWorkers(), is(equalTo(TestDataProvider.TEST_WORKERS)));
        assertThat(actualBufferPluginSetting.getPipelineName(), is(equalTo(TestDataProvider.TEST_PIPELINE_NAME)));
        assertThat(actualBufferPluginSetting.getNumberOfProcessWorkers(), is(equalTo(TestDataProvider.TEST_WORKERS)));
        actualProcesserPluginSettings.forEach(processorPluginSetting -> {
            assertThat(processorPluginSetting.getPipelineName(), is(equalTo(TestDataProvider.TEST_PIPELINE_NAME)));
            assertThat(processorPluginSetting.getNumberOfProcessWorkers(), is(equalTo(TestDataProvider.TEST_WORKERS)));
        });
        actualSinkPluginSettings.forEach(sinkPluginSetting -> {
            assertThat(sinkPluginSetting.getPipelineName(), is(equalTo(TestDataProvider.TEST_PIPELINE_NAME)));
            assertThat(sinkPluginSetting.getNumberOfProcessWorkers(), is(equalTo(TestDataProvider.TEST_WORKERS)));
        });
    }

    @Test
    public void testExceptionThrownWhenPrepperAndProcessorAreConfigured() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> new PipelineConfiguration(
                TestDataProvider.validSingleConfiguration(),
                null,
                TestDataProvider.validMultipleConfigurationOfSizeOne(),
                TestDataProvider.validMultipleConfigurationOfSizeOne(),
                TestDataProvider.validMultipleConfiguration(),
                TestDataProvider.TEST_WORKERS,
                TestDataProvider.TEST_DELAY));

        final String expected = "Pipeline configuration cannot specify a prepper and processor configuration. It is " +
                "recommended to move prepper configurations to the processor section to maintain compatibility with " +
                "DataPrepper version 1.2 and above.";

        assertTrue(exception.getMessage().contains(expected));
    }

    private void assertEqualProcessorPluginSettings(
            final List<Map.Entry<String, Map<String, Object>>> expectedPluginSettings,
            final List<PluginSetting> actualPluginSettings) {
        assertEquals(expectedPluginSettings.size(), actualPluginSettings.size());

        expectedPluginSettings.forEach(expectedSetting -> {
            final PluginSetting actualSetting = actualPluginSettings.stream()
                    .filter(plugin -> expectedSetting.getKey().equals(plugin.getName()))
                    .findFirst()
                    .orElseThrow(() -> new InputMismatchException("Expected setting named " + expectedSetting.getKey()));

            final Map<String, Object> expectedSettingValue = expectedSetting.getValue();
            final Set<String> expectedKeySet = expectedSettingValue.keySet();
            final Map<String, Object> settings = actualSetting.getSettings();
            assertEquals(expectedKeySet.size(), settings.size());

            expectedKeySet.forEach(key -> {
                assertEquals(expectedSettingValue.get(key), settings.get(key));
            });
        });
    }

    @Test
    public void testPipelineConfigurationWithPrepperOrProcessorAreEquivalent() {
        Map.Entry<String, Map<String, Object>> sourcePluginSettings = TestDataProvider.validSingleConfiguration();
        List<Map.Entry<String, Map<String, Object>>> expectedPluginSettings = TestDataProvider.validMultipleConfigurationOfSizeOne();

        PipelineConfiguration prepperConfig = new PipelineConfiguration(
                sourcePluginSettings,
                null,
                expectedPluginSettings,
                null,
                TestDataProvider.validMultipleConfiguration(),
                TestDataProvider.TEST_WORKERS,
                TestDataProvider.TEST_DELAY);
        PipelineConfiguration processorConfig = new PipelineConfiguration(
                sourcePluginSettings,
                null,
                null,
                expectedPluginSettings,
                TestDataProvider.validMultipleConfiguration(),
                TestDataProvider.TEST_WORKERS,
                TestDataProvider.TEST_DELAY);

        assertEqualProcessorPluginSettings(expectedPluginSettings, prepperConfig.getProcessorPluginSettings());
        assertEqualProcessorPluginSettings(expectedPluginSettings, processorConfig.getProcessorPluginSettings());
    }

    @Test
    public void testOnlySourceAndSink() {
        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(TestDataProvider.validSingleConfiguration(),
                null,
                null,
                TestDataProvider.validMultipleConfigurationOfSizeOne(),
                null, null);
        final PluginSetting actualSourcePluginSetting = pipelineConfiguration.getSourcePluginSetting();
        final PluginSetting actualBufferPluginSetting = pipelineConfiguration.getBufferPluginSetting();
        final List<PluginSetting> actualProcessorPluginSettings = pipelineConfiguration.getProcessorPluginSettings();
        final List<PluginSetting> actualSinkPluginSettings = pipelineConfiguration.getSinkPluginSettings();

        comparePluginSettings(actualSourcePluginSetting, TestDataProvider.VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getBufferPluginSetting(), notNullValue());
        comparePluginSettings(actualBufferPluginSetting, BlockingBuffer.getDefaultPluginSettings());
        assertThat(actualProcessorPluginSettings, isA(Iterable.class));
        assertThat(actualProcessorPluginSettings.size(), is(0));
        assertThat(actualSinkPluginSettings.size(), is(1));
        comparePluginSettings(actualSinkPluginSettings.get(0), TestDataProvider.VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getWorkers(), CoreMatchers.is(TestDataProvider.DEFAULT_WORKERS));
        assertThat(pipelineConfiguration.getReadBatchDelay(), CoreMatchers.is(TestDataProvider.DEFAULT_READ_BATCH_DELAY));
    }

    @Test //not using expected to assert the message
    public void testNoSourceConfiguration() {
        try {
            new PipelineConfiguration(
                    null,
                    TestDataProvider.validSingleConfiguration(),
                    TestDataProvider.validMultipleConfiguration(),
                    TestDataProvider.validMultipleConfiguration(),
                    TestDataProvider.TEST_WORKERS, TestDataProvider.TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, source is a required component"));
        }
    }

    @Test
    public void testNoProcessorAndNoPrepperConfiguration() {
        final PipelineConfiguration nullProcessorConfiguration = new PipelineConfiguration(
                TestDataProvider.validSingleConfiguration(),
                TestDataProvider.validSingleConfiguration(),
                null,
                TestDataProvider.validMultipleConfiguration(),
                TestDataProvider.TEST_WORKERS, TestDataProvider.TEST_DELAY);
        assertThat(nullProcessorConfiguration.getProcessorPluginSettings(), isA(Iterable.class));
        assertThat(nullProcessorConfiguration.getProcessorPluginSettings().size(), is(0));

        final PipelineConfiguration emptyProcessorsConfiguration = new PipelineConfiguration(
                TestDataProvider.validSingleConfiguration(),
                TestDataProvider.validSingleConfiguration(),
                new ArrayList<>(),
                TestDataProvider.validMultipleConfiguration(),
                TestDataProvider.TEST_WORKERS, TestDataProvider.TEST_DELAY);
        assertThat(emptyProcessorsConfiguration.getProcessorPluginSettings(), isA(Iterable.class));
        assertThat(emptyProcessorsConfiguration.getProcessorPluginSettings().size(), is(0));
    }

    @Test //not using expected to assert the message
    public void testNoSinkConfiguration() {
        try {
            new PipelineConfiguration(
                    TestDataProvider.validSingleConfiguration(),
                    TestDataProvider.validSingleConfiguration(),
                    TestDataProvider.validMultipleConfiguration(),
                    null,
                    TestDataProvider.TEST_WORKERS, TestDataProvider.TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, at least one sink is required"));
        }

        try {
            new PipelineConfiguration(
                    TestDataProvider.validSingleConfiguration(),
                    TestDataProvider.validSingleConfiguration(),
                    TestDataProvider.validMultipleConfiguration(),
                    new ArrayList<>(),
                    TestDataProvider.TEST_WORKERS, TestDataProvider.TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, at least one sink is required"));
        }
    }

    @Test //not using expected to assert the message
    public void testInvalidWorkersConfiguration() {
        try {
            new PipelineConfiguration(
                    TestDataProvider.validSingleConfiguration(),
                    TestDataProvider.validSingleConfiguration(),
                    TestDataProvider.validMultipleConfiguration(),
                    TestDataProvider.validMultipleConfiguration(),
                    0, TestDataProvider.TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, workers cannot be 0"));
        }
    }

    @Test //not using expected to assert the message
    public void testInvalidDelayConfiguration() {
        try {
            new PipelineConfiguration(
                    TestDataProvider.validSingleConfiguration(),
                    TestDataProvider.validSingleConfiguration(),
                    null,
                    TestDataProvider.validMultipleConfiguration(),
                    TestDataProvider.validMultipleConfiguration(),
                    TestDataProvider.TEST_WORKERS, 0);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, delay cannot be 0"));
        }
    }

    @Test
    public void testPipelineConfigurationWithoutPluginSettingAttributes() throws Exception {
        final Map<String, PipelineConfiguration> pipelineConfigurationMap = TestDataProvider.readConfigFile(
                TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE);
        assertThat(pipelineConfigurationMap.size(), is(equalTo(1)));
        final PipelineConfiguration actualPipelineConfiguration = pipelineConfigurationMap.get(TestDataProvider.TEST_PIPELINE_NAME);
        assertThat(actualPipelineConfiguration, notNullValue());
        assertThat(actualPipelineConfiguration.getSourcePluginSetting(), notNullValue());
        assertThat(actualPipelineConfiguration.getBufferPluginSetting(), notNullValue());
        assertThat(actualPipelineConfiguration.getProcessorPluginSettings(), notNullValue());
        assertThat(actualPipelineConfiguration.getProcessorPluginSettings().size(), is(equalTo(0)));
        assertThat(actualPipelineConfiguration.getSinkPluginSettings(), notNullValue());
        assertThat(actualPipelineConfiguration.getSinkPluginSettings().size(), is(equalTo(1)));
    }

    private void comparePluginSettings(final PluginSetting actual, final PluginSetting expected) {
        assertThat("Plugin names are different", actual.getName(), is(expected.getName()));
        final Map<String, Object> actualSettings = actual.getSettings();
        final Map<String, Object> expectedSettings = expected.getSettings();
        assertThat("Plugin settings have different number of attributes", actualSettings.size(), is(expectedSettings.size()));
        actualSettings.forEach((key, value) -> {
            assertThat(actualSettings.get(key), is(expectedSettings.get(key))); //all tests use string values so equals is fine
        });
    }
}
