/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.configuration;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineModelTest {

    public static Random random = new Random();
    public static final Integer TEST_WORKERS = random.nextInt(30);
    public static final Integer TEST_READ_BATCH_DELAY = random.nextInt(40);
    public static PluginModel TEST_VALID_SOURCE_PLUGIN_MODEL = new PluginModel("source-plugin", validPluginSettings());
    public static PluginModel TEST_VALID_BUFFER_PLUGIN_MODEL = new PluginModel("buffer", validPluginSettings());
    public static PluginModel TEST_VALID_PREPPERS_PLUGIN_MODEL = new PluginModel("prepper", validPluginSettings());
    public static PluginModel TEST_VALID_SINKS_PLUGIN_MODEL = new PluginModel("sink", validPluginSettings());

    @Test
    void testPipelineModelCreation() {
        final PipelineModel pipelineModel = new PipelineModel(
                validSourcePluginModel(),
                validBufferPluginModel(),
                validPreppersPluginModel(),
                validPipelineRouter(),
                validSinksPluginModel(),
                TEST_WORKERS,
                TEST_READ_BATCH_DELAY
        );
        final PluginModel originalSource = pipelineModel.getSource();
        final PluginModel originalBuffer = pipelineModel.getBuffer();
        final List<PluginModel> originalPreppers = pipelineModel.getProcessors();
        final List<SinkModel> originalSinks = pipelineModel.getSinks();

        assertThat(originalSource, notNullValue());
        assertThat(originalBuffer, notNullValue());
        assertThat(originalPreppers, notNullValue());
        assertThat(originalSinks, notNullValue());
        assertThat(originalSource.getPluginName(), is(equalTo(TEST_VALID_SOURCE_PLUGIN_MODEL.getPluginName())));
        assertThat(originalSource.getPluginSettings(), is(equalTo(TEST_VALID_SOURCE_PLUGIN_MODEL.getPluginSettings())));
        assertThat(originalBuffer.getPluginName(), is(equalTo(TEST_VALID_BUFFER_PLUGIN_MODEL.getPluginName())));
        assertThat(originalBuffer.getPluginSettings(), is(equalTo(TEST_VALID_BUFFER_PLUGIN_MODEL.getPluginSettings())));
        assertThat(originalPreppers.get(0).getPluginName(), is(equalTo(TEST_VALID_PREPPERS_PLUGIN_MODEL.getPluginName())));
        assertThat(originalPreppers.get(0).getPluginSettings(), is(equalTo(TEST_VALID_PREPPERS_PLUGIN_MODEL.getPluginSettings())));
        assertThat(pipelineModel.getRoutes(), notNullValue());
        assertThat(pipelineModel.getRoutes().size(), equalTo(1));
        assertThat(originalSinks.get(0).getPluginName(), is(equalTo(TEST_VALID_SINKS_PLUGIN_MODEL.getPluginName())));
        assertThat(originalSinks.get(0).getPluginSettings(), is(equalTo(TEST_VALID_SINKS_PLUGIN_MODEL.getPluginSettings())));
        assertThat(pipelineModel.getWorkers(), is(TEST_WORKERS));
        assertThat(pipelineModel.getReadBatchDelay(), is(TEST_READ_BATCH_DELAY));
    }

    static Map<String, Object> validPluginSettings() {
        final Map<String, Object> settings = new HashMap<>();
        settings.put("property", "value");
        return settings;
    }

    static PluginModel validSourcePluginModel() {
        return new PluginModel("source-plugin", validPluginSettings());
    }

    static PluginModel validBufferPluginModel() {
        return new PluginModel("buffer", validPluginSettings());
    }

    static List<PluginModel> validPreppersPluginModel() {
        return Collections.singletonList(new PluginModel("prepper", validPluginSettings()));
    }

    private static List<ConditionalRoute> validPipelineRouter() {
        return Collections.singletonList(new ConditionalRoute("my-route", "/a==b"));
    }

    static List<SinkModel> validSinksPluginModel() {
        return Collections.singletonList(SinkModel.builder(new PluginModel("sink", validPluginSettings())).build());
    }

    @Test
    void testPipelineModelWithValidProcessorConfig() {
        final List<PluginModel> expectedPreppersPluginModel = validPreppersPluginModel();
        final PipelineModel pipelineModel = new PipelineModel(
                validSourcePluginModel(),
                null,
                expectedPreppersPluginModel,
                validPipelineRouter(),
                validSinksPluginModel(),
                TEST_WORKERS,
                TEST_READ_BATCH_DELAY
        );

        assertEquals(expectedPreppersPluginModel, pipelineModel.getProcessors());
    }

    @Test
    void testPipelineModelWithNullSinksThrowsException() {
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> new PipelineModel(
                validSourcePluginModel(),
                validBufferPluginModel(),
                validPreppersPluginModel(),
                validPipelineRouter(),
                null,
                TEST_WORKERS,
                TEST_READ_BATCH_DELAY
        ));

        final String expected = "Sinks must not be null";

        assertTrue(exception.getMessage().contains(expected));
    }

    @Test
    void testPipelineModelWithEmptySinksThrowsException() {
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> new PipelineModel(
                validSourcePluginModel(),
                validBufferPluginModel(),
                validPreppersPluginModel(),
                validPipelineRouter(),
                Collections.emptyList(),
                TEST_WORKERS,
                TEST_READ_BATCH_DELAY
        ));

        final String expected = "PipelineModel must include at least 1 sink";

        assertThat(exception.getMessage(), equalTo(expected));
    }

    @Test
    void testPipelineModelCreation_with_null_router_creates_model_with_empty_router() {
        final PipelineModel pipelineModel = new PipelineModel(
                validSourcePluginModel(),
                validBufferPluginModel(),
                validPreppersPluginModel(),
                null,
                validSinksPluginModel(),
                TEST_WORKERS,
                TEST_READ_BATCH_DELAY
        );
        final PluginModel originalSource = pipelineModel.getSource();
        final PluginModel originalBuffer = pipelineModel.getBuffer();
        final List<PluginModel> originalPreppers = pipelineModel.getProcessors();
        final List<SinkModel> originalSinks = pipelineModel.getSinks();

        assertThat(originalSource, notNullValue());
        assertThat(originalBuffer, notNullValue());
        assertThat(originalPreppers, notNullValue());
        assertThat(originalSinks, notNullValue());
        assertThat(originalSource.getPluginName(), is(equalTo(TEST_VALID_SOURCE_PLUGIN_MODEL.getPluginName())));
        assertThat(originalSource.getPluginSettings(), is(equalTo(TEST_VALID_SOURCE_PLUGIN_MODEL.getPluginSettings())));
        assertThat(originalBuffer.getPluginName(), is(equalTo(TEST_VALID_BUFFER_PLUGIN_MODEL.getPluginName())));
        assertThat(originalBuffer.getPluginSettings(), is(equalTo(TEST_VALID_BUFFER_PLUGIN_MODEL.getPluginSettings())));
        assertThat(originalPreppers.get(0).getPluginName(), is(equalTo(TEST_VALID_PREPPERS_PLUGIN_MODEL.getPluginName())));
        assertThat(originalPreppers.get(0).getPluginSettings(), is(equalTo(TEST_VALID_PREPPERS_PLUGIN_MODEL.getPluginSettings())));
        assertThat(originalSinks.get(0).getPluginName(), is(equalTo(TEST_VALID_SINKS_PLUGIN_MODEL.getPluginName())));
        assertThat(originalSinks.get(0).getPluginSettings(), is(equalTo(TEST_VALID_SINKS_PLUGIN_MODEL.getPluginSettings())));
        assertThat(pipelineModel.getWorkers(), is(TEST_WORKERS));
        assertThat(pipelineModel.getReadBatchDelay(), is(TEST_READ_BATCH_DELAY));

        assertThat(pipelineModel.getRoutes(), notNullValue());
        assertThat(pipelineModel.getRoutes().size(), equalTo(0));
    }

}
