/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.parser.model;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.amazon.dataprepper.TestDataProvider.DEFAULT_READ_BATCH_DELAY;
import static com.amazon.dataprepper.TestDataProvider.DEFAULT_WORKERS;
import static com.amazon.dataprepper.TestDataProvider.TEST_DELAY;
import static com.amazon.dataprepper.TestDataProvider.TEST_PIPELINE_NAME;
import static com.amazon.dataprepper.TestDataProvider.TEST_WORKERS;
import static com.amazon.dataprepper.TestDataProvider.VALID_PLUGIN_SETTING_1;
import static com.amazon.dataprepper.TestDataProvider.VALID_PLUGIN_SETTING_2;
import static com.amazon.dataprepper.TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE;
import static com.amazon.dataprepper.TestDataProvider.readConfigFile;
import static com.amazon.dataprepper.TestDataProvider.validMultipleConfiguration;
import static com.amazon.dataprepper.TestDataProvider.validMultipleConfigurationOfSizeOne;
import static com.amazon.dataprepper.TestDataProvider.validSingleConfiguration;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class PipelineConfigurationTests {

    @Test
    public void testPipelineConfigurationCreation() {
        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(validSingleConfiguration(),
                null,
                validMultipleConfigurationOfSizeOne(),
                validMultipleConfiguration(),
                TEST_WORKERS, TEST_DELAY);
        final PluginSetting actualSourcePluginSetting = pipelineConfiguration.getSourcePluginSetting();
        final PluginSetting actualBufferPluginSetting = pipelineConfiguration.getBufferPluginSetting();
        final List<PluginSetting> actualPrepperPluginSettings = pipelineConfiguration.getPrepperPluginSettings();
        final List<PluginSetting> actualSinkPluginSettings = pipelineConfiguration.getSinkPluginSettings();

        comparePluginSettings(actualSourcePluginSetting, VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getBufferPluginSetting(), notNullValue());
        comparePluginSettings(actualBufferPluginSetting, BlockingBuffer.getDefaultPluginSettings());
        assertThat(actualPrepperPluginSettings.size(), is(1));
        actualPrepperPluginSettings.forEach(prepperSettings -> comparePluginSettings(prepperSettings, VALID_PLUGIN_SETTING_1));
        assertThat(actualSinkPluginSettings.size(), is(2));
        comparePluginSettings(actualSinkPluginSettings.get(0), VALID_PLUGIN_SETTING_1);
        comparePluginSettings(actualSinkPluginSettings.get(1), VALID_PLUGIN_SETTING_2);
        assertThat(pipelineConfiguration.getWorkers(), is(TEST_WORKERS));
        assertThat(pipelineConfiguration.getReadBatchDelay(), is(TEST_DELAY));

        pipelineConfiguration.updateCommonPipelineConfiguration(TEST_PIPELINE_NAME);
        assertThat(actualSourcePluginSetting.getPipelineName(), is(equalTo(TEST_PIPELINE_NAME)));
        assertThat(actualSourcePluginSetting.getNumberOfProcessWorkers(), is(equalTo(TEST_WORKERS)));
        assertThat(actualBufferPluginSetting.getPipelineName(), is(equalTo(TEST_PIPELINE_NAME)));
        assertThat(actualBufferPluginSetting.getNumberOfProcessWorkers(), is(equalTo(TEST_WORKERS)));
        actualPrepperPluginSettings.forEach(prepperPluginSetting -> {
            assertThat(prepperPluginSetting.getPipelineName(), is(equalTo(TEST_PIPELINE_NAME)));
            assertThat(prepperPluginSetting.getNumberOfProcessWorkers(), is(equalTo(TEST_WORKERS)));
        });
        actualSinkPluginSettings.forEach(sinkPluginSetting -> {
            assertThat(sinkPluginSetting.getPipelineName(), is(equalTo(TEST_PIPELINE_NAME)));
            assertThat(sinkPluginSetting.getNumberOfProcessWorkers(), is(equalTo(TEST_WORKERS)));
        });
    }

    @Test
    public void testOnlySourceAndSink() {
        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(validSingleConfiguration(),
                null,
                null,
                validMultipleConfigurationOfSizeOne(),
                null, null);
        final PluginSetting actualSourcePluginSetting = pipelineConfiguration.getSourcePluginSetting();
        final PluginSetting actualBufferPluginSetting = pipelineConfiguration.getBufferPluginSetting();
        final List<PluginSetting> actualPrepperPluginSettings = pipelineConfiguration.getPrepperPluginSettings();
        final List<PluginSetting> actualSinkPluginSettings = pipelineConfiguration.getSinkPluginSettings();

        comparePluginSettings(actualSourcePluginSetting, VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getBufferPluginSetting(), notNullValue());
        comparePluginSettings(actualBufferPluginSetting, BlockingBuffer.getDefaultPluginSettings());
        assertThat(actualPrepperPluginSettings, isA(Iterable.class));
        assertThat(actualPrepperPluginSettings.size(), is(0));
        assertThat(actualSinkPluginSettings.size(), is(1));
        comparePluginSettings(actualSinkPluginSettings.get(0), VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getWorkers(), is(DEFAULT_WORKERS));
        assertThat(pipelineConfiguration.getReadBatchDelay(), is(DEFAULT_READ_BATCH_DELAY));
    }

    @Test //not using expected to assert the message
    public void testNoSourceConfiguration() {
        try {
            new PipelineConfiguration(
                    null,
                    validSingleConfiguration(),
                    validMultipleConfiguration(),
                    validMultipleConfiguration(),
                    TEST_WORKERS, TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, source is a required component"));
        }
    }

    @Test
    public void testNoPrepperConfiguration() {
        final PipelineConfiguration nullPreppersConfiguration = new PipelineConfiguration(
                validSingleConfiguration(),
                validSingleConfiguration(),
                null,
                validMultipleConfiguration(),
                TEST_WORKERS, TEST_DELAY);
        assertThat(nullPreppersConfiguration.getPrepperPluginSettings(), isA(Iterable.class));
        assertThat(nullPreppersConfiguration.getPrepperPluginSettings().size(), is(0));

        final PipelineConfiguration emptyPreppersConfiguration = new PipelineConfiguration(
                validSingleConfiguration(),
                validSingleConfiguration(),
                new ArrayList<>(),
                validMultipleConfiguration(),
                TEST_WORKERS, TEST_DELAY);
        assertThat(emptyPreppersConfiguration.getPrepperPluginSettings(), isA(Iterable.class));
        assertThat(emptyPreppersConfiguration.getPrepperPluginSettings().size(), is(0));
    }

    @Test //not using expected to assert the message
    public void testNoSinkConfiguration() {
        try {
            new PipelineConfiguration(
                    validSingleConfiguration(),
                    validSingleConfiguration(),
                    validMultipleConfiguration(),
                    null,
                    TEST_WORKERS, TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, at least one sink is required"));
        }

        try {
            new PipelineConfiguration(
                    validSingleConfiguration(),
                    validSingleConfiguration(),
                    validMultipleConfiguration(),
                    new ArrayList<>(),
                    TEST_WORKERS, TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, at least one sink is required"));
        }
    }

    @Test //not using expected to assert the message
    public void testInvalidWorkersConfiguration() {
        try {
            new PipelineConfiguration(
                    validSingleConfiguration(),
                    validSingleConfiguration(),
                    validMultipleConfiguration(),
                    validMultipleConfiguration(),
                    0, TEST_DELAY);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, workers cannot be 0"));
        }
    }

    @Test //not using expected to assert the message
    public void testInvalidDelayConfiguration() {
        try {
            new PipelineConfiguration(
                    validSingleConfiguration(),
                    validSingleConfiguration(),
                    validMultipleConfiguration(),
                    validMultipleConfiguration(),
                    TEST_WORKERS, 0);
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, delay cannot be 0"));
        }
    }

    @Test
    public void testPipelineConfigurationWithoutPluginSettingAttributes() throws Exception {
        final Map<String, PipelineConfiguration> pipelineConfigurationMap = readConfigFile(
                VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE);
        assertThat(pipelineConfigurationMap.size(), is(equalTo(1)));
        final PipelineConfiguration actualPipelineConfiguration = pipelineConfigurationMap.get(TEST_PIPELINE_NAME);
        assertThat(actualPipelineConfiguration, notNullValue());
        assertThat(actualPipelineConfiguration.getSourcePluginSetting(), notNullValue());
        assertThat(actualPipelineConfiguration.getBufferPluginSetting(), notNullValue());
        assertThat(actualPipelineConfiguration.getPrepperPluginSettings(), notNullValue());
        assertThat(actualPipelineConfiguration.getPrepperPluginSettings().size(), is(equalTo(0)));
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
