/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser.model;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.pipeline.parser.TestConfigurationProvider;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.configuration.SinkModel;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelineConfigurationTests {

    private PluginModel source;
    private List<PluginModel> processors;
    private List<SinkModel> sinks;

    @BeforeEach
    void setUp() {
        source = TestConfigurationProvider.validSingleConfiguration();
        processors = TestConfigurationProvider.validMultipleConfigurationOfSizeOne();
        sinks = TestConfigurationProvider.validMultipleSinkConfiguration();
    }

    @Test
    void testPipelineConfigurationCreation() {
        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getSource()).thenReturn(source);
        when(pipelineModel.getProcessors()).thenReturn(processors);
        when(pipelineModel.getSinks()).thenReturn(sinks);
        when(pipelineModel.getWorkers()).thenReturn(TestConfigurationProvider.TEST_WORKERS);
        when(pipelineModel.getReadBatchDelay()).thenReturn(TestConfigurationProvider.TEST_DELAY);
        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(pipelineModel);

        final PluginSetting actualSourcePluginSetting = pipelineConfiguration.getSourcePluginSetting();
        final PluginSetting actualBufferPluginSetting = pipelineConfiguration.getBufferPluginSetting();
        final List<PluginSetting> actualProcesserPluginSettings = pipelineConfiguration.getProcessorPluginSettings();
        final List<SinkContextPluginSetting> actualSinkPluginSettings = pipelineConfiguration.getSinkPluginSettings();

        comparePluginSettings(actualSourcePluginSetting, TestConfigurationProvider.VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getBufferPluginSetting(), notNullValue());
        comparePluginSettings(actualBufferPluginSetting, BlockingBuffer.getDefaultPluginSettings());
        assertThat(actualProcesserPluginSettings.size(), is(1));
        actualProcesserPluginSettings.forEach(processorSettings -> comparePluginSettings(processorSettings, TestConfigurationProvider.VALID_PLUGIN_SETTING_1));
        assertThat(actualSinkPluginSettings.size(), is(2));
        comparePluginSettings(actualSinkPluginSettings.get(0), TestConfigurationProvider.VALID_PLUGIN_SETTING_1);
        comparePluginSettings(actualSinkPluginSettings.get(1), TestConfigurationProvider.VALID_PLUGIN_SETTING_2);
        assertThat(pipelineConfiguration.getWorkers(), CoreMatchers.is(TestConfigurationProvider.TEST_WORKERS));
        assertThat(pipelineConfiguration.getReadBatchDelay(), CoreMatchers.is(TestConfigurationProvider.TEST_DELAY));

        pipelineConfiguration.updateCommonPipelineConfiguration(TestConfigurationProvider.TEST_PIPELINE_NAME);
        assertThat(actualSourcePluginSetting.getPipelineName(), is(equalTo(TestConfigurationProvider.TEST_PIPELINE_NAME)));
        assertThat(actualSourcePluginSetting.getNumberOfProcessWorkers(), is(equalTo(TestConfigurationProvider.TEST_WORKERS)));
        assertThat(actualBufferPluginSetting.getPipelineName(), is(equalTo(TestConfigurationProvider.TEST_PIPELINE_NAME)));
        assertThat(actualBufferPluginSetting.getNumberOfProcessWorkers(), is(equalTo(TestConfigurationProvider.TEST_WORKERS)));
        actualProcesserPluginSettings.forEach(processorPluginSetting -> {
            assertThat(processorPluginSetting.getPipelineName(), is(equalTo(TestConfigurationProvider.TEST_PIPELINE_NAME)));
            assertThat(processorPluginSetting.getNumberOfProcessWorkers(), is(equalTo(TestConfigurationProvider.TEST_WORKERS)));
        });
        actualSinkPluginSettings.forEach(sinkPluginSetting -> {
            assertThat(sinkPluginSetting.getPipelineName(), is(equalTo(TestConfigurationProvider.TEST_PIPELINE_NAME)));
            assertThat(sinkPluginSetting.getNumberOfProcessWorkers(), is(equalTo(TestConfigurationProvider.TEST_WORKERS)));
        });
    }

    @Test
    void testOnlySourceAndSink() {
        sinks = TestConfigurationProvider.validMultipleSinkConfigurationOfSizeOne();
        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getSource()).thenReturn(source);
        when(pipelineModel.getSinks()).thenReturn(sinks);
        when(pipelineModel.getProcessors()).thenReturn(null);
        when(pipelineModel.getWorkers()).thenReturn(null);
        when(pipelineModel.getReadBatchDelay()).thenReturn(null);
        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(pipelineModel);
        final PluginSetting actualSourcePluginSetting = pipelineConfiguration.getSourcePluginSetting();
        final PluginSetting actualBufferPluginSetting = pipelineConfiguration.getBufferPluginSetting();
        final List<PluginSetting> actualProcessorPluginSettings = pipelineConfiguration.getProcessorPluginSettings();
        final List<SinkContextPluginSetting> actualSinkPluginSettings = pipelineConfiguration.getSinkPluginSettings();

        comparePluginSettings(actualSourcePluginSetting, TestConfigurationProvider.VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getBufferPluginSetting(), notNullValue());
        comparePluginSettings(actualBufferPluginSetting, BlockingBuffer.getDefaultPluginSettings());
        assertThat(actualProcessorPluginSettings, isA(Iterable.class));
        assertThat(actualProcessorPluginSettings.size(), is(0));
        assertThat(actualSinkPluginSettings.size(), is(1));
        comparePluginSettings(actualSinkPluginSettings.get(0), TestConfigurationProvider.VALID_PLUGIN_SETTING_1);
        assertThat(pipelineConfiguration.getWorkers(), CoreMatchers.is(TestConfigurationProvider.DEFAULT_WORKERS));
        assertThat(pipelineConfiguration.getReadBatchDelay(), CoreMatchers.is(TestConfigurationProvider.DEFAULT_READ_BATCH_DELAY));
    }

    @Test
    void testNoSourceConfiguration() {
        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getProcessors()).thenReturn(processors);
        when(pipelineModel.getSinks()).thenReturn(sinks);
        when(pipelineModel.getWorkers()).thenReturn(TestConfigurationProvider.TEST_WORKERS);
        when(pipelineModel.getReadBatchDelay()).thenReturn(TestConfigurationProvider.TEST_DELAY);

        final IllegalArgumentException actual = assertThrows(IllegalArgumentException.class, () -> new PipelineConfiguration(pipelineModel));

        assertThat(actual.getMessage(), equalTo("Invalid configuration, source is a required component"));
    }

    @Test
    void testNullProcessorConfiguration() {
        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getSource()).thenReturn(source);
        when(pipelineModel.getProcessors()).thenReturn(null);
        when(pipelineModel.getSinks()).thenReturn(sinks);
        when(pipelineModel.getWorkers()).thenReturn(TestConfigurationProvider.TEST_WORKERS);
        when(pipelineModel.getReadBatchDelay()).thenReturn(TestConfigurationProvider.TEST_DELAY);
        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(pipelineModel);
        assertThat(pipelineConfiguration.getProcessorPluginSettings(), isA(Iterable.class));
        assertThat(pipelineConfiguration.getProcessorPluginSettings().size(), is(0));
    }

    @Test
    void testEmptyProcessorConfiguration() {
        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getSource()).thenReturn(source);
        when(pipelineModel.getProcessors()).thenReturn(new ArrayList<>());
        when(pipelineModel.getSinks()).thenReturn(sinks);
        when(pipelineModel.getWorkers()).thenReturn(TestConfigurationProvider.TEST_WORKERS);
        when(pipelineModel.getReadBatchDelay()).thenReturn(TestConfigurationProvider.TEST_DELAY);
        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(pipelineModel);
        assertThat(pipelineConfiguration.getProcessorPluginSettings(), isA(Iterable.class));
        assertThat(pipelineConfiguration.getProcessorPluginSettings().size(), is(0));
    }

    @Test
    void testNullSinkConfiguration() {
        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getSource()).thenReturn(source);
        when(pipelineModel.getProcessors()).thenReturn(processors);
        when(pipelineModel.getSinks()).thenReturn(Collections.emptyList());
        when(pipelineModel.getWorkers()).thenReturn(TestConfigurationProvider.TEST_WORKERS);
        when(pipelineModel.getReadBatchDelay()).thenReturn(TestConfigurationProvider.TEST_DELAY);

        final IllegalArgumentException actual = assertThrows(IllegalArgumentException.class, () -> new PipelineConfiguration(pipelineModel));

        assertThat(actual.getMessage(), equalTo("Invalid configuration, at least one sink is required"));
    }

    @Test
    void testEmptySinkConfiguration() {
        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getSource()).thenReturn(source);
        when(pipelineModel.getProcessors()).thenReturn(processors);
        when(pipelineModel.getSinks()).thenReturn(new ArrayList<>());
        when(pipelineModel.getWorkers()).thenReturn(TestConfigurationProvider.TEST_WORKERS);
        when(pipelineModel.getReadBatchDelay()).thenReturn(TestConfigurationProvider.TEST_DELAY);

        final IllegalArgumentException actual = assertThrows(IllegalArgumentException.class, () -> new PipelineConfiguration(pipelineModel));

        assertThat(actual.getMessage(), equalTo("Invalid configuration, at least one sink is required"));
    }

    @Test
    void testInvalidWorkersConfiguration() {
        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getSource()).thenReturn(source);
        when(pipelineModel.getProcessors()).thenReturn(processors);
        when(pipelineModel.getSinks()).thenReturn(sinks);
        when(pipelineModel.getWorkers()).thenReturn(0);
        when(pipelineModel.getReadBatchDelay()).thenReturn(TestConfigurationProvider.TEST_DELAY);
        final IllegalArgumentException actual = assertThrows(IllegalArgumentException.class, () -> new PipelineConfiguration(pipelineModel));
        assertThat(actual.getMessage(), equalTo("Invalid configuration, workers cannot be 0"));
    }

    @Test
    void testInvalidDelayConfiguration() {
        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getSource()).thenReturn(source);
        when(pipelineModel.getProcessors()).thenReturn(processors);
        when(pipelineModel.getSinks()).thenReturn(sinks);
        when(pipelineModel.getWorkers()).thenReturn(TestConfigurationProvider.TEST_WORKERS);
        when(pipelineModel.getReadBatchDelay()).thenReturn(-1);
        final IllegalArgumentException actual = assertThrows(IllegalArgumentException.class, () -> new PipelineConfiguration(pipelineModel));
        assertThat(actual.getMessage(), equalTo("Invalid configuration, delay must be a non-negative integer."));
    }

    @Test
    void testSinksWithRoutes() {
        final List<Collection<String>> orderedSinkRoutes = new ArrayList<>();
        for (final SinkModel sink : sinks) {
            final Set<String> routes = Collections.singleton(UUID.randomUUID().toString());
            when(sink.getRoutes()).thenReturn(routes);
            orderedSinkRoutes.add(routes);
        }

        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getSource()).thenReturn(source);
        when(pipelineModel.getSinks()).thenReturn(sinks);
        when(pipelineModel.getProcessors()).thenReturn(null);
        when(pipelineModel.getWorkers()).thenReturn(null);
        when(pipelineModel.getReadBatchDelay()).thenReturn(null);

        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(pipelineModel);

        final List<SinkContextPluginSetting> actualSinkPluginSettings = pipelineConfiguration.getSinkPluginSettings();

        assertThat(actualSinkPluginSettings.size(), equalTo(2));
        comparePluginSettings(actualSinkPluginSettings.get(0), TestConfigurationProvider.VALID_PLUGIN_SETTING_1);
        comparePluginSettings(actualSinkPluginSettings.get(1), TestConfigurationProvider.VALID_PLUGIN_SETTING_2);
        assertThat(actualSinkPluginSettings.get(0).getSinkContext().getRoutes(), equalTo(orderedSinkRoutes.get(0)));
        assertThat(actualSinkPluginSettings.get(1).getSinkContext().getRoutes(), equalTo(orderedSinkRoutes.get(1)));
    }

    @Test
    void testSinksWithTagsTargetKey() {
        final List<String> orderedSinkTagTagets = new ArrayList<>();
        for (final SinkModel sink : sinks) {
            final String tagsTargetKey = UUID.randomUUID().toString();
            when(sink.getTagsTargetKey()).thenReturn(tagsTargetKey);
            orderedSinkTagTagets.add(tagsTargetKey);
        }

        final PipelineModel pipelineModel = mock(PipelineModel.class);
        when(pipelineModel.getSource()).thenReturn(source);
        when(pipelineModel.getSinks()).thenReturn(sinks);
        when(pipelineModel.getProcessors()).thenReturn(null);
        when(pipelineModel.getWorkers()).thenReturn(null);
        when(pipelineModel.getReadBatchDelay()).thenReturn(null);

        final PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(pipelineModel);

        final List<SinkContextPluginSetting> actualSinkPluginSettings = pipelineConfiguration.getSinkPluginSettings();

        assertThat(actualSinkPluginSettings.size(), equalTo(2));
        comparePluginSettings(actualSinkPluginSettings.get(0), TestConfigurationProvider.VALID_PLUGIN_SETTING_1);
        comparePluginSettings(actualSinkPluginSettings.get(1), TestConfigurationProvider.VALID_PLUGIN_SETTING_2);
        assertThat(actualSinkPluginSettings.get(0).getSinkContext().getTagsTargetKey(), equalTo(orderedSinkTagTagets.get(0)));
        assertThat(actualSinkPluginSettings.get(1).getSinkContext().getTagsTargetKey(), equalTo(orderedSinkTagTagets.get(1)));
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
