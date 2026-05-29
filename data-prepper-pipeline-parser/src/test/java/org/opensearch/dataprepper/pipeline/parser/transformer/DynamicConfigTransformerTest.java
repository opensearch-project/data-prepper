/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationFileReader;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationReader;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;
import org.opensearch.dataprepper.pipeline.parser.TestConfigurationProvider;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleStream;

import java.util.Arrays;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DynamicConfigTransformerTest {

    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

    TransformersFactory transformersFactory;
    RuleEvaluator ruleEvaluator;

    @Test
    void test_invokeMethod_throws_when_functionProviders_is_null() {
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                transformer.invokeMethod(null, "someMethod", String.class, "arg"));
        assertTrue(exception.getMessage().contains("function_providers cannot be empty"));
    }

    @Test
    void test_invokeMethod_throws_when_functionProviders_is_empty() {
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                transformer.invokeMethod(Collections.emptyList(), "someMethod", String.class, "arg"));
        assertTrue(exception.getMessage().contains("function_providers cannot be empty"));
    }

    @Test
    void test_invokeMethod_throws_when_class_does_not_implement_interface() {
        // java.lang.String does NOT implement PipelineTransformFunctionProvider.
        // resolveMethod should reject it with the interface check.
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        List<String> providers = Collections.singletonList("java.lang.String");
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                transformer.invokeMethod(providers, "valueOf", String.class, "test"));
        assertTrue(exception.getMessage().contains("does not implement PipelineTransformFunctionProvider"),
                "Expected interface check failure, got: " + exception.getMessage());
    }

    @Test
    void test_invokeMethod_throws_when_method_not_annotated() {
        // ValidProviderNoAnnotation implements the interface but its method
        // lacks @TransformationFunction. invokeMethod should reject it.
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        List<String> providers = Collections.singletonList(
                "org.opensearch.dataprepper.pipeline.parser.transformer.DynamicConfigTransformerTest$ValidProviderNoAnnotation");
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                transformer.invokeMethod(providers, "unannotatedMethod", String.class, "arg"));
        assertTrue(exception.getMessage().contains("is not annotated with @TransformationFunction"),
                "Expected annotation check failure, got: " + exception.getMessage());
    }


    @Test
    void test_successful_transformation_with_only_source_and_sink() throws IOException {
        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String expectedDocDBFilePath = TestConfigurationProvider.EXPECTED_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);
        InputStream ruleStream = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream);
        List<RuleStream> ruleStreams = Collections.singletonList(ruleInputStream);
        when(transformersFactory.loadRules()).thenReturn(ruleStreams);
        when(transformersFactory.getPluginTemplateFileStream(pluginName)).thenReturn(templateStream);

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration(pipelinesDataFlowModel);
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);
        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }

    @Test
    void test_successful_transformation_with_documentdb() throws IOException {

        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCUMENTDB_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCUMENTDB_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String expectedDocDBFilePath = TestConfigurationProvider.EXPECTED_TRANSFORMATION_DOCUMENTDB_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);
        Path ruleFile = mock(Path.class);
        when(ruleFile.getFileName()).thenReturn(Paths.get(ruleDocDBFilePath).getFileName());
        InputStream ruleStream = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream);
        List<RuleStream> ruleStreams = Collections.singletonList(ruleInputStream);
        when(transformersFactory.loadRules()).thenReturn(ruleStreams);
        when(transformersFactory.getPluginTemplateFileStream(pluginName)).thenReturn(templateStream);
        ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration(pipelinesDataFlowModel);
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);
        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }

    @RepeatedTest(5)
    void test_successful_transformation_with_subpipelines() throws IOException {

        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCUMENTDB_SUBPIPELINES_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCUMENTDB_SUBPIPELINES_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String expectedDocDBFilePath = TestConfigurationProvider.EXPECTED_TRANSFORMATION_DOCUMENTDB_SUBPIPLINES_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        InputStream ruleStream1 = new FileInputStream(ruleDocDBFilePath);
        InputStream ruleStream2 = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream1 = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream1);
        RuleStream ruleInputStream2 = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream2);

        List<RuleStream> ruleStreams1 = Collections.singletonList(ruleInputStream1);
        List<RuleStream> ruleStreams2 = Collections.singletonList(ruleInputStream2);
        when(transformersFactory.loadRules()).thenReturn(ruleStreams1).thenReturn(ruleStreams2);
        when(transformersFactory.getPluginTemplateFileStream(pluginName)).thenReturn(templateStream);

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration(pipelinesDataFlowModel);
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);
        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }

    @Test
    void test_successful_transformation_with_functionPlaceholder() throws IOException {
        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCUMENTDB_FUNCTION_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCUMENTDB_FUNCTION_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String expectedDocDBFilePath = TestConfigurationProvider.EXPECTED_TRANSFORMATION_DOCUMENTDB_FUNCTION_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        InputStream ruleStream = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream);

        List<RuleStream> ruleStreams = Collections.singletonList(ruleInputStream);
        when(transformersFactory.loadRules()).thenReturn(ruleStreams);
        when(transformersFactory.getPluginTemplateFileStream(pluginName)).thenReturn(templateStream);

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration(pipelinesDataFlowModel);
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);
        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }

    @Test
    void test_successful_transformation_with_complete_template() throws IOException {
        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCDB2_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCDB2_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String expectedDocDBFilePath = TestConfigurationProvider.EXPECTED_TRANSFORMATION_DOCDB2_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        InputStream ruleStream = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream);

        List<RuleStream> ruleStreams = Collections.singletonList(ruleInputStream);
        when(transformersFactory.loadRules()).thenReturn(ruleStreams);
        when(transformersFactory.getPluginTemplateFileStream(pluginName)).thenReturn(templateStream);

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration(pipelinesDataFlowModel);
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);

        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }


    @Test
    void test_successful_transformation_with_routes_keyword() throws IOException {
        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCUMENTDB_ROUTES_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCUMENTDB_FINAL_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String expectedDocDBFilePath = TestConfigurationProvider.EXPECTED_TRANSFORMATION_DOCUMENTDB_ROUTES_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        InputStream ruleStream = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream);

        List<RuleStream> ruleStreams = Collections.singletonList(ruleInputStream);
        when(transformersFactory.loadRules()).thenReturn(ruleStreams);
        when(transformersFactory.getPluginTemplateFileStream(pluginName)).thenReturn(templateStream);

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration(pipelinesDataFlowModel);
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);

        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }

    @Test
    void test_successful_transformation_with_route_keyword() throws IOException {
        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCUMENTDB_ROUTE_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCUMENTDB_FINAL_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        //should be same as with routes keyword
        String expectedDocDBFilePath = TestConfigurationProvider.EXPECTED_TRANSFORMATION_DOCUMENTDB_ROUTES_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        InputStream ruleStream = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream);

        List<RuleStream> ruleStreams = Collections.singletonList(ruleInputStream);
        when(transformersFactory.loadRules()).thenReturn(ruleStreams);
        when(transformersFactory.getPluginTemplateFileStream(pluginName)).thenReturn(templateStream);

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration(pipelinesDataFlowModel);
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);

        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }

    @Test
    void test_successful_transformation_with_routes_and_subpipelines() throws IOException {
        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCUMENTDB_SUBPIPELINES_ROUTES_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCUMENTDB_FINAL_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String expectedDocDBFilePath = TestConfigurationProvider.EXPECTED_TRANSFORMATION_DOCUMENTDB_SUBPIPELINES_ROUTES_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        InputStream ruleStream1 = new FileInputStream(ruleDocDBFilePath);
        InputStream ruleStream2 = new FileInputStream(ruleDocDBFilePath);
        InputStream ruleStream3 = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream1 = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream1);
        RuleStream ruleInputStream2 = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream2);
        RuleStream ruleInputStream3 = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream3);

        List<RuleStream> ruleStreams1 = Collections.singletonList(ruleInputStream1);
        List<RuleStream> ruleStreams2 = Collections.singletonList(ruleInputStream2);
        List<RuleStream> ruleStreams3 = Collections.singletonList(ruleInputStream3);

        when(transformersFactory.loadRules()).thenReturn(ruleStreams1).thenReturn(ruleStreams2).thenReturn(ruleStreams3);
        when(transformersFactory.getPluginTemplateFileStream(pluginName)).thenReturn(templateStream);

        RuleEvaluator ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration(pipelinesDataFlowModel);
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);

        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }

    @Test
    void testInvalidJsonPathThrowsException() throws IOException {
        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCDB_SIMPLE_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCDB_SIMPLE_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        TransformersFactory transformersFactory = mock(TransformersFactory.class);

        InputStream ruleStream = new FileInputStream(ruleDocDBFilePath);
        InputStream templateStream = new FileInputStream(templateDocDBFilePath);
        RuleStream ruleInputStream = new RuleStream(Paths.get(ruleDocDBFilePath).getFileName().toString(), ruleStream);

        List<RuleStream> ruleStreams = Collections.singletonList(ruleInputStream);
        when(transformersFactory.loadRules()).thenReturn(ruleStreams);
        when(transformersFactory.getPluginTemplateFileStream(pluginName)).thenReturn(templateStream);

        ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        assertThrows(RuntimeException.class, () -> transformer.transformConfiguration(pipelinesDataFlowModel));
    }


    @Test
    void test_overlay_directive_merges_into_opensearch_sinks() throws Exception {
        ObjectMapper jsonMapper = new ObjectMapper();
        ObjectNode root = jsonMapper.createObjectNode();
        ObjectNode templatePipelines = root.putObject("templatePipelines");
        ObjectNode pipeline = templatePipelines.putObject("test-s3");

        ArrayNode sinkArray = pipeline.putArray("sink");
        ObjectNode osEntry = sinkArray.addObject();
        ObjectNode osConfig = osEntry.putObject("opensearch");
        osConfig.put("hosts", "https://localhost:9200");
        osConfig.put("index", "my-index");
        ObjectNode s3Entry = sinkArray.addObject();
        s3Entry.putObject("s3").put("bucket", "my-bucket");

        ObjectNode overlay = pipeline.putObject("<<overlay sink[*].opensearch>>");
        overlay.put("action", "upsert");
        overlay.put("document_id", "test-id");

        DynamicConfigTransformer transformer = new DynamicConfigTransformer(mock(RuleEvaluator.class));
        Method method = DynamicConfigTransformer.class.getDeclaredMethod(
                "processOverlayDirectives", JsonNode.class, String.class, List.class);
        method.setAccessible(true);
        method.invoke(transformer, root, "{}", Collections.emptyList());

        JsonNode resultOs = sinkArray.get(0).get("opensearch");
        assertThat(resultOs.get("hosts").asText()).isEqualTo("https://localhost:9200");
        assertThat(resultOs.get("index").asText()).isEqualTo("my-index");
        assertThat(resultOs.get("action").asText()).isEqualTo("upsert");
        assertThat(resultOs.get("document_id").asText()).isEqualTo("test-id");

        assertThat(sinkArray.get(1).get("s3").has("action")).isFalse();
        assertThat(pipeline.has("<<overlay sink[*].opensearch>>")).isFalse();
    }

    @Test
    void test_overlay_directive_overrides_existing_fields() throws Exception {
        ObjectMapper jsonMapper = new ObjectMapper();
        ObjectNode root = jsonMapper.createObjectNode();
        ObjectNode templatePipelines = root.putObject("templatePipelines");
        ObjectNode pipeline = templatePipelines.putObject("test-s3");

        ArrayNode sinkArray = pipeline.putArray("sink");
        ObjectNode osEntry = sinkArray.addObject();
        ObjectNode osConfig = osEntry.putObject("opensearch");
        osConfig.put("hosts", "https://localhost:9200");
        osConfig.put("action", "index");
        ObjectNode existingScript = osConfig.putObject("script");
        existingScript.put("custom_field", "should-be-replaced");

        ObjectNode overlay = pipeline.putObject("<<overlay sink[*].opensearch>>");
        overlay.put("action", "upsert");
        ObjectNode overlayScript = overlay.putObject("script");
        overlayScript.put("source", "ctx._source.merge(params.doc)");

        DynamicConfigTransformer transformer = new DynamicConfigTransformer(mock(RuleEvaluator.class));
        Method method = DynamicConfigTransformer.class.getDeclaredMethod(
                "processOverlayDirectives", JsonNode.class, String.class, List.class);
        method.setAccessible(true);
        method.invoke(transformer, root, "{}", Collections.emptyList());

        JsonNode resultOs = sinkArray.get(0).get("opensearch");
        assertThat(resultOs.get("hosts").asText()).isEqualTo("https://localhost:9200");
        assertThat(resultOs.get("action").asText()).isEqualTo("upsert");
        assertThat(resultOs.get("script").get("source").asText()).isEqualTo("ctx._source.merge(params.doc)");
        assertThat(resultOs.get("script").has("custom_field")).isFalse();
    }

    private static final String PROVIDER_PKG = "org.opensearch.dataprepper.pipeline.parser.transformer.dataprepper_transformer.";

    @Test
    void test_invokeMethod_succeeds_with_valid_provider_and_annotated_method() throws Exception {
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        List<String> providers = Collections.singletonList(PROVIDER_PKG + "ValidAnnotatedProvider");
        Object result = transformer.invokeMethod(providers, "transformValue", String.class, "hello");
        assertEquals("HELLO", result);
    }

    @Test
    void test_invokeMethod_throws_when_class_does_not_exist() {
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        List<String> providers = Collections.singletonList("com.example.does.not.Exist");
        Exception exception = assertThrows(Exception.class, () ->
                transformer.invokeMethod(providers, "someMethod", String.class, "arg"));
        // Class.forName fails, wraps into a ReflectiveOperationException (ClassNotFoundException)
        assertTrue(exception instanceof ClassNotFoundException ||
                exception.getCause() instanceof ClassNotFoundException ||
                exception.getMessage().contains("Exist"),
                "Expected ClassNotFoundException for non-existent class, got: " + exception);
    }

    @Test
    void test_invokeMethod_resolves_method_from_second_provider_when_first_lacks_it() throws Exception {
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        // ProviderWithoutTargetMethod has 'otherMethod' but not 'transformValue'
        // ValidAnnotatedProvider has 'transformValue'
        List<String> providers = Arrays.asList(
                PROVIDER_PKG + "ProviderWithoutTargetMethod",
                PROVIDER_PKG + "ValidAnnotatedProvider");
        Object result = transformer.invokeMethod(providers, "transformValue", String.class, "world");
        assertEquals("WORLD", result);
    }

    @Test
    void test_invokeMethod_throws_when_annotated_method_is_non_static() {
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        // ProviderWithNonStaticMethod has 'instanceMethod' which is non-static but annotated.
        // getMethod finds it (public), annotation check passes, but invoke(null, arg) fails
        // because you cannot invoke a non-static method on null.
        List<String> providers = Collections.singletonList(PROVIDER_PKG + "ProviderWithNonStaticMethod");
        Exception exception = assertThrows(Exception.class, () ->
                transformer.invokeMethod(providers, "instanceMethod", String.class, "arg"));
        // Should get a NullPointerException or IllegalArgumentException from Method.invoke(null, ...)
        assertTrue(exception instanceof NullPointerException ||
                exception instanceof IllegalArgumentException ||
                exception.getCause() instanceof NullPointerException ||
                exception.getCause() instanceof IllegalArgumentException,
                "Expected invocation failure for non-static method, got: " + exception);
    }

    @Test
    void test_invokeMethod_rejects_class_not_implementing_interface_without_running_static_init() {
        // NonProviderWithStaticInit does NOT implement PipelineTransformFunctionProvider
        // and has a static initializer that sets a system property.
        // Class.forName(name, false, classLoader) should NOT run the static initializer.
        // resolveMethod should reject it with the interface check.
        System.clearProperty("test.static.init.ran");

        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        List<String> providers = Collections.singletonList(PROVIDER_PKG + "NonProviderWithStaticInit");

        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                transformer.invokeMethod(providers, "getValue", String.class, "arg"));

        assertTrue(exception.getMessage().contains("does not implement PipelineTransformFunctionProvider"),
                "Expected interface check failure, got: " + exception.getMessage());

        // Verify static initializer was NOT executed (Class.forName with initialize=false)
        assertTrue(System.getProperty("test.static.init.ran") == null,
                "Static initializer should not have run when Class.forName uses initialize=false");
    }

    @Test
    void test_invokeMethod_throws_when_no_provider_has_the_method() {
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        // Both providers implement the interface but neither has 'nonExistentMethod'
        List<String> providers = Arrays.asList(
                PROVIDER_PKG + "ProviderWithoutTargetMethod",
                PROVIDER_PKG + "ValidAnnotatedProvider");
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                transformer.invokeMethod(providers, "nonExistentMethod", String.class, "arg"));
        assertTrue(exception.getMessage().contains("Could not find a class with method"),
                "Expected 'could not find' message, got: " + exception.getMessage());
    }

    public static class ValidProviderNoAnnotation implements org.opensearch.dataprepper.model.plugin.PipelineTransformFunctionProvider {
        public static String unannotatedMethod(String input) {
            return input;
        }
    }
}
