/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RuleEvaluatorTest {

    @Test
    void test_isTransformationNeeded_ForDocDBSource_ShouldReturn_True() throws IOException {

        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCUMENTDB_CONFIG_FILE;
        String ruleDocDBTemplatePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String pluginName = "documentdb";
        String pipelineName = "test-pipeline";
        Map<String, Object> sourceOptions = new HashMap<>();
        Map<String, Object> s3Bucket = new HashMap<>();
        s3Bucket.put("s3_bucket", "bucket-name");
        sourceOptions.put("s3_bucket", s3Bucket);
        final PluginModel source = new PluginModel(pluginName, sourceOptions);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        TransformersFactory transformersFactory = mock(TransformersFactory.class);
//        TransformersFactory transformersFactory = spy(new TransformersFactory("", ""));

        InputStream ruleStream = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(ruleDocDBTemplatePath);
        RuleStream ruleInputStream = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream);

        List<RuleStream> ruleStreams = Collections.singletonList(ruleInputStream);
        when(transformersFactory.loadRules()).thenReturn(ruleStreams);
        when(transformersFactory.getPluginTemplateFileStream(pluginName)).thenReturn(templateStream);

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        RuleEvaluatorResult result = ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);

        assertTrue(result.isEvaluatedResult());
        assertEquals(result.getPipelineName(), pipelineName);
    }

    @Test
    void test_isTransformationNeeded_ForOtherSource_ShouldReturn_False() throws IOException {

        String pluginName = "http";
        String pipelineName = "test-pipeline";
        Map<String, Object> sourceOptions = new HashMap<>();
        Map<String, Object> s3Bucket = new HashMap<>();
        s3Bucket.put("s3_bucket", "bucket-name");
        sourceOptions.put("s3_bucket", s3Bucket);
        final PluginModel source = new PluginModel(pluginName, sourceOptions);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);
        RuleEvaluatorResult result = ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);

        assertFalse(result.isEvaluatedResult());
    }

    @Test
    void testThrowsExceptionOnFileError() {
        TransformersFactory transformersFactory = mock(TransformersFactory.class);
        String pipelineName = "test-pipeline";
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("option1", "1");
        sourceOptions.put("option2", null);
        final PluginModel source = new PluginModel("http", sourceOptions);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        when(transformersFactory.loadRules()).thenThrow(new RuntimeException("File not found"));

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        Exception exception = assertThrows(RuntimeException.class, () -> {
            ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);
        });

        assertEquals("File not found", exception.getMessage());
    }
}
