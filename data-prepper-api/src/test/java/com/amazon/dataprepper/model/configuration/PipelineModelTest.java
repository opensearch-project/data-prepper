package com.amazon.dataprepper.model.configuration;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class PipelineModelTest {

    public static Random random = new Random();
    public static final Integer TEST_WORKERS = random.nextInt(30);
    public static final Integer TEST_READ_BATCH_DELAY = random.nextInt(40);
    public static PluginModel TEST_VALID_SOURCE_PLUGIN_MODEL = new PluginModel("source-plugin", validPluginSettings());
    public static PluginModel TEST_VALID_PREPPERS_PLUGIN_MODEL = new PluginModel("prepper", validPluginSettings());
    public static PluginModel TEST_VALID_SINKS_PLUGIN_MODEL = new PluginModel("sink", validPluginSettings());

    @Test
    public void testPipelineModelCreation() {
        final PipelineModel pipelineModel = new PipelineModel(
                validSourcePluginModel(),
                validPreppersPluginModel(),
                validSinksPluginModel(),
                TEST_WORKERS,
                TEST_READ_BATCH_DELAY
        );
        final PluginModel originalSourcePluginModel = pipelineModel.getSourcePluginModel();
        final List<PluginModel> originalPreppersPluginModel = pipelineModel.getPrepperPluginModel();
        final List<PluginModel> originalSinksPluginModel = pipelineModel.getSinkPluginModel();

        assertThat(originalSourcePluginModel, notNullValue());
        assertThat(originalPreppersPluginModel, notNullValue());
        assertThat(originalSinksPluginModel, notNullValue());
        assertThat(originalSourcePluginModel.getPluginName(), is(equalTo(TEST_VALID_SOURCE_PLUGIN_MODEL.getPluginName())));
        assertThat(originalSourcePluginModel.getPluginSettings(), is(equalTo(TEST_VALID_SOURCE_PLUGIN_MODEL.getPluginSettings())));
        assertThat(originalPreppersPluginModel.get(0).getPluginName(), is(equalTo(TEST_VALID_PREPPERS_PLUGIN_MODEL.getPluginName())));
        assertThat(originalPreppersPluginModel.get(0).getPluginSettings(), is(equalTo(TEST_VALID_PREPPERS_PLUGIN_MODEL.getPluginSettings())));
        assertThat(originalSinksPluginModel.get(0).getPluginName(), is(equalTo(TEST_VALID_SINKS_PLUGIN_MODEL.getPluginName())));
        assertThat(originalSinksPluginModel.get(0).getPluginSettings(), is(equalTo(TEST_VALID_SINKS_PLUGIN_MODEL.getPluginSettings())));
        assertThat(pipelineModel.getWorkers(), is(TEST_WORKERS));
        assertThat(pipelineModel.getReadBatchDelay(), is(TEST_READ_BATCH_DELAY));
    }

    public static Map<String, Object> validPluginSettings() {
        final Map<String, Object> settings = new HashMap<>();
        settings.put("property", "value");
        return settings;
    }

    public static PluginModel validSourcePluginModel() {
        return new PluginModel("source-plugin", validPluginSettings());
    }

    public static List<PluginModel> validPreppersPluginModel() {
        return Collections.singletonList(new PluginModel("prepper", validPluginSettings()));
    }

    public static List<PluginModel> validSinksPluginModel() {
        return Collections.singletonList(new PluginModel("sink", validPluginSettings()));
    }
}
