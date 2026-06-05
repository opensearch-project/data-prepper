/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import org.junit.jupiter.api.Test;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    void test_moreSpecificRule_matchesFirst_whenMultipleRulesApply() throws IOException {
        // rds-joins rule has 2 conditions, rds rule has 1 condition.
        // Both match an RDS pipeline with joins config.
        // The more specific rule (rds-joins) should win.
        String rdsRulePath = "src/test/resources/transformation/rules/rds-rule.yaml";
        String rdsJoinsRulePath = "src/test/resources/transformation/rules/rds-joins-rule.yaml";
        String templatePath = "src/test/resources/transformation/templates/testSource/rds-joins-template.yaml";

        String pipelineName = "test-pipeline";
        Map<String, Object> joinsConfig = new HashMap<>();
        joinsConfig.put("version_field", "__versions");
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("joins", joinsConfig);
        final PluginModel source = new PluginModel("rds", sourceOptions);
        final List<SinkModel> sinks = Collections.singletonList(
                new SinkModel("testSink", Collections.emptyList(), null,
                        Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(
                source, null, Collections.emptyList(), null, sinks, 8, 50);
        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        // Load both rules — rds rule first (less specific), rds-joins second (more specific)
        RuleStream rdsRule = new RuleStream("rds-rule.yaml", new FileInputStream(rdsRulePath));
        RuleStream rdsJoinsRule = new RuleStream("rds-joins-rule.yaml", new FileInputStream(rdsJoinsRulePath));
        when(transformersFactory.loadRules()).thenReturn(List.of(rdsRule, rdsJoinsRule));
        when(transformersFactory.getPluginTemplateFileStream("rds-joins"))
                .thenReturn(new FileInputStream(templatePath));

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);
        RuleEvaluatorResult result = ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);

        assertTrue(result.isEvaluatedResult());
        assertEquals(pipelineName, result.getPipelineName());
        // The rds-joins template should be selected, not the generic rds template
        assertTrue(result.getPipelineTemplateModel() != null);
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

    @Test
    void test_validateFunctionProviders_rejects_invalid_package() {
        // Create a rule YAML with a function_provider that doesn't contain 'dataprepper_transformer'
        String invalidRuleYaml = "apply_when:\n" +
                "  - \"$.test-pipeline.source.documentdb\"\n" +
                "plugin_name: \"documentdb\"\n" +
                "function_providers:\n" +
                "  - \"com.example.invalid.SomeClass\"\n";

        TransformersFactory transformersFactory = mock(TransformersFactory.class);
        InputStream ruleStream = new java.io.ByteArrayInputStream(invalidRuleYaml.getBytes());
        RuleStream ruleInputStream = new RuleStream("invalid-rule.yaml", ruleStream);
        when(transformersFactory.loadRules()).thenReturn(Collections.singletonList(ruleInputStream));

        // Build a pipeline that would match the rule's apply_when path
        String pipelineName = "test-pipeline";
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("s3_bucket", "bucket-name");
        final PluginModel source = new PluginModel("documentdb", sourceOptions);
        final List<SinkModel> sinks = Collections.singletonList(
                new SinkModel("testSink", Collections.emptyList(), null,
                        Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(
                source, null, Collections.emptyList(), null, sinks, 8, 50);
        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Validation happens when rules are loaded during isTransformationNeeded
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel));

        assertTrue(exception.getMessage().contains("Invalid function_provider"),
                "Expected package validation failure, got: " + exception.getMessage());
        assertTrue(exception.getMessage().contains("dataprepper_transformer"),
                "Expected message to mention required package segment");
    }

    @Test
    void test_validateFunctionProviders_accepts_valid_package() throws IOException {
        // Create a rule YAML with a valid function_provider containing 'dataprepper_transformer'
        String validRuleYaml = "apply_when:\n" +
                "  - \"$.test-pipeline.source.documentdb\"\n" +
                "plugin_name: \"documentdb\"\n" +
                "function_providers:\n" +
                "  - \"org.opensearch.dataprepper.pipeline.parser.transformer.dataprepper_transformer.TestTransformFunctionProvider\"\n";

        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCDB1_CONFIG_FILE;

        TransformersFactory transformersFactory = mock(TransformersFactory.class);
        InputStream ruleStream = new java.io.ByteArrayInputStream(validRuleYaml.getBytes());
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream = new RuleStream("valid-rule.yaml", ruleStream);
        when(transformersFactory.loadRules()).thenReturn(Collections.singletonList(ruleInputStream));
        when(transformersFactory.getPluginTemplateFileStream("documentdb")).thenReturn(templateStream);

        // Build a matching pipeline
        String pipelineName = "test-pipeline";
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("s3_bucket", "bucket-name");
        final PluginModel source = new PluginModel("documentdb", sourceOptions);
        final List<SinkModel> sinks = Collections.singletonList(
                new SinkModel("testSink", Collections.emptyList(), null,
                        Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(
                source, null, Collections.emptyList(), null, sinks, 8, 50);
        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Should NOT throw — valid package name
        RuleEvaluatorResult result = ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);
        assertTrue(result.isEvaluatedResult());
    }

    @Test
    void test_validateFunctionProviders_allows_null_providers() throws IOException {
        // Rule YAML without function_providers field should be fine
        String ruleYaml = "apply_when:\n" +
                "  - \"$.test-pipeline.source.documentdb\"\n" +
                "plugin_name: \"documentdb\"\n";

        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCDB1_CONFIG_FILE;

        TransformersFactory transformersFactory = mock(TransformersFactory.class);
        InputStream ruleStream = new java.io.ByteArrayInputStream(ruleYaml.getBytes());
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream = new RuleStream("no-providers-rule.yaml", ruleStream);
        when(transformersFactory.loadRules()).thenReturn(Collections.singletonList(ruleInputStream));
        when(transformersFactory.getPluginTemplateFileStream("documentdb")).thenReturn(templateStream);

        // Build a matching pipeline
        String pipelineName = "test-pipeline";
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("s3_bucket", "bucket-name");
        final PluginModel source = new PluginModel("documentdb", sourceOptions);
        final List<SinkModel> sinks = Collections.singletonList(
                new SinkModel("testSink", Collections.emptyList(), null,
                        Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(
                source, null, Collections.emptyList(), null, sinks, 8, 50);
        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Should NOT throw — null function_providers is valid
        RuleEvaluatorResult result = ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel);
        assertTrue(result.isEvaluatedResult());
    }

    @Test
    void test_validateFunctionProviders_rejects_class_not_implementing_interface() {
        // NonProviderWithStaticInit is in dataprepper_transformer package but doesn't implement the interface
        String ruleYaml = "apply_when:\n" +
                "  - \"$.test-pipeline.source.documentdb\"\n" +
                "plugin_name: \"documentdb\"\n" +
                "function_providers:\n" +
                "  - \"org.opensearch.dataprepper.pipeline.parser.transformer.dataprepper_transformer.NonProviderWithStaticInit\"\n";

        TransformersFactory transformersFactory = mock(TransformersFactory.class);
        InputStream ruleStream = new java.io.ByteArrayInputStream(ruleYaml.getBytes());
        RuleStream ruleInputStream = new RuleStream("bad-interface-rule.yaml", ruleStream);
        when(transformersFactory.loadRules()).thenReturn(Collections.singletonList(ruleInputStream));

        String pipelineName = "test-pipeline";
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("s3_bucket", "bucket-name");
        final PluginModel source = new PluginModel("documentdb", sourceOptions);
        final List<SinkModel> sinks = Collections.singletonList(
                new SinkModel("testSink", Collections.emptyList(), null,
                        Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(
                source, null, Collections.emptyList(), null, sinks, 8, 50);
        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel));

        assertTrue(exception.getMessage().contains("does not implement PipelineTransformFunctionProvider"),
                "Expected interface validation failure, got: " + exception.getMessage());
    }

    @Test
    void test_validateFunctionProviders_rejects_class_with_no_annotated_methods() {
        // ProviderWithNoAnnotatedMethods is in the dataprepper_transformer package
        // and implements PipelineTransformFunctionProvider, but has NO methods
        // annotated with @TransformationFunction.
        String fqcn = "org.opensearch.dataprepper.pipeline.parser.transformer.dataprepper_transformer.ProviderWithNoAnnotatedMethods";
        String ruleYaml = "apply_when:\n" +
                "  - \"$.test-pipeline.source.documentdb\"\n" +
                "plugin_name: \"documentdb\"\n" +
                "function_providers:\n" +
                "  - \"" + fqcn + "\"\n";

        TransformersFactory transformersFactory = mock(TransformersFactory.class);
        InputStream ruleStream = new java.io.ByteArrayInputStream(ruleYaml.getBytes());
        RuleStream ruleInputStream = new RuleStream("no-annotation-rule.yaml", ruleStream);
        when(transformersFactory.loadRules()).thenReturn(Collections.singletonList(ruleInputStream));

        String pipelineName = "test-pipeline";
        Map<String, Object> sourceOptions = new HashMap<>();
        sourceOptions.put("s3_bucket", "bucket-name");
        final PluginModel source = new PluginModel("documentdb", sourceOptions);
        final List<SinkModel> sinks = Collections.singletonList(
                new SinkModel("testSink", Collections.emptyList(), null,
                        Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(
                source, null, Collections.emptyList(), null, sinks, 8, 50);
        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel));

        assertTrue(exception.getMessage().contains("has no methods annotated with @TransformationFunction"),
                "Expected annotation validation failure, got: " + exception.getMessage());
    }
}
