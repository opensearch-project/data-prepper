/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.SinkModel;
import org.opensearch.dataprepper.pipeline.parser.TestConfigurationProvider;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RuleEvaluatorTest {

    @Test
    void test_isTransformationNeeded_ForDocDBSource_ShouldReturn_True() throws FileNotFoundException {

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

        InputStream ruleStream = new FileInputStream(ruleDocDBFilePath);
        when(transformersFactory.getPluginRuleFileStream(pluginName)).thenReturn(ruleStream);

        RuleEvaluatorResult result = ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);

        // Assert
        assertTrue(result.isEvaluatedResult());
        assertEquals(result.getPipelineName(), pipelineName);
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
    void testThrowsExceptionOnFileError() {
        TransformersFactory transformersFactory = mock(TransformersFactory.class);
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

        // Setup mock to throw an exception when file path is incorrect
        when(transformersFactory.getPluginRuleFileLocation("documentdb")).thenThrow(new RuntimeException("File not found"));
        when(transformersFactory.getPluginRuleFileStream("documentdb")).thenThrow(new RuntimeException("File not found"));

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Execute and Assert
        Exception exception = assertThrows(RuntimeException.class, () -> {
            ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);
        });
        assertEquals("File not found", exception.getMessage());
    }
}
