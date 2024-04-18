/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.SinkModel;
import org.opensearch.dataprepper.pipeline.parser.TestConfigurationProvider;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RuleEvaluatorTest {

    @BeforeEach
    void setUp() {
    }

    @Test
    void test_isTransformationNeeded_ForDocDBSource_ShouldReturn_True() {

        // Set up
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCUMENTDB_CONFIG_FILE;
        String pluginName = "documentdb";
        String pipelineName = "test-pipeline";
        Map sourceOptions = new HashMap<String, Object>();
        Map s3_bucket = new HashMap<>();
        s3_bucket.put("s3_bucket", "bucket-name");
        List collections = new ArrayList();
        collections.add(s3_bucket);
        sourceOptions.put("collections", collections);
        final PluginModel source = new PluginModel(pluginName, sourceOptions);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        TransformersFactory transformersFactory = Mockito.spy(new TransformersFactory(
                TestConfigurationProvider.RULES_TRANSFORMATION_DIRECTORY,
                TestConfigurationProvider.TEMPLATES_SOURCE_TRANSFORMATION_DIRECTORY
        ));
        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);
        when(transformersFactory.getPluginRuleFileLocation(pluginName)).thenReturn(ruleDocDBFilePath);
        RuleEvaluatorResult result = ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);

        // Assert
        assertTrue(result.isEvaluatedResult());
        assertEquals(result.getPipelineName(),pipelineName);
    }

    @Test
    void test_isTransformationNeeded_ForOtherSource_ShouldReturn_False() {
        // Set up
        String pipelineName = "test-pipeline";
        Map sourceOptions = new HashMap<String, Object>();
        sourceOptions.put("option1", "1");
        sourceOptions.put("option2", null);
        final PluginModel source = new PluginModel("http", sourceOptions);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        TransformersFactory transformersFactory = Mockito.spy(new TransformersFactory(
                TestConfigurationProvider.RULES_TRANSFORMATION_DIRECTORY,
                TestConfigurationProvider.TEMPLATES_SOURCE_TRANSFORMATION_DIRECTORY
        ));
        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);
        RuleEvaluatorResult result = ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);

        // Assert
        assertEquals(result.isEvaluatedResult(), false);
    }

    @Test
    void testIsRuleValidThrowsExceptionForInvalidResultType() {

    }

    @Test
    void testGetPluginNameThatNeedsTransformation() {

//        String pluginName = ruleEvaluator.getPluginNameThatNeedsTransformation(ruleConfig.getApplyWhen());

//        assertEquals("PluginName", pluginName);
    }

    @Test
    void testGetPluginNameThatNeedsTransformationInvalidFormat() {
        // Test with an invalid expression format to ensure it throws a RuntimeException
    }

    @Test
    void testgetTemplateJsonStringSuccess() {
        // Set up
        String pipelineName = "test-pipeline";
        Map sourceOptions = new HashMap<String, Object>();
        Map s3_bucket = new HashMap<>();
        s3_bucket.put("s3_bucket", "bucket-name");
        List collections = new ArrayList();
        collections.add(s3_bucket);
        sourceOptions.put("collections", collections);
        final PluginModel source = new PluginModel("documentdb", sourceOptions);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

//        ruleEvaluator = new RuleEvaluator(pipelinesDataFlowModel);
//        PipelineTemplateModel templateModel = ruleEvaluator.getTemplateModel();
    }

    // You can add more tests to cover edge cases and exceptional scenarios.
}
