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
import static org.mockito.ArgumentMatchers.eq;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
        Map<String, Object> s3_bucket = new HashMap<>();
        s3_bucket.put("s3_bucket", "bucket-name");
        List<Map<String, Object>> collections = new ArrayList<>();
        collections.add(s3_bucket);
        sourceOptions.put("collections", collections);
        final PluginModel source = new PluginModel(pluginName, sourceOptions);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        Path ruleFile = mock(Path.class);
        List<Path> ruleFiles = Collections.singletonList(ruleFile);
        InputStream ruleStream = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(ruleDocDBTemplatePath);
        when(ruleFile.getFileName()).thenReturn(Paths.get("documentdb-rule.yaml").getFileName());
        when(transformersFactory.getRuleFiles()).thenReturn(ruleFiles);
        when(transformersFactory.readRuleFile(eq(ruleFile))).thenReturn(ruleStream);
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
        Map<String, Object> s3_bucket = new HashMap<>();
        s3_bucket.put("s3_bucket", "bucket-name");
        List<Map<String, Object>> collections = new ArrayList<>();
        collections.add(s3_bucket);
        sourceOptions.put("collections", collections);
        final PluginModel source = new PluginModel(pluginName, sourceOptions);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        when(transformersFactory.getRuleFiles()).thenReturn(List.of());


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

        when(transformersFactory.getRuleFiles()).thenThrow(new RuntimeException("File not found"));

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        Exception exception = assertThrows(RuntimeException.class, () -> {
            ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);
        });

        assertEquals("File not found", exception.getMessage());
    }
}
