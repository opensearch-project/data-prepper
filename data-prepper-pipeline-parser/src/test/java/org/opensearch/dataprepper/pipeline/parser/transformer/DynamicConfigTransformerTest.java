/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationFileReader;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationReader;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;
import org.opensearch.dataprepper.pipeline.parser.TestConfigurationProvider;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DynamicConfigTransformerTest {

    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

    TransformersFactory transformersFactory;
    RuleEvaluator ruleEvaluator;

    @Test
    void test_getAccountIdFromRole_returns_account_id_from_valid_role_arn() {
        final String testAccountId = RandomStringUtils.randomNumeric(12);
        final String testRoleArn = String.format("arn:aws:iam::%s:role/example-role", testAccountId);
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        assertThat(transformer.getAccountIdFromRole(testRoleArn)).isEqualTo(testAccountId);
    }

    @ParameterizedTest
    @MethodSource("providesInvalidRoleArn")
    void test_getAccountIdFromRole_returns_null_from_invalid_role_arn(final String testRoleArn) {
        ruleEvaluator = mock(RuleEvaluator.class);
        DynamicConfigTransformer transformer = new DynamicConfigTransformer(ruleEvaluator);
        assertThat(transformer.getAccountIdFromRole(testRoleArn)).isNull();
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

    private static Stream<Arguments> providesInvalidRoleArn() {
        return Stream.of(
                null,
                Arguments.of("arn:aws:iam:::role/test-role"),
                Arguments.of("invalid-format-arn")
        );
    }
}
