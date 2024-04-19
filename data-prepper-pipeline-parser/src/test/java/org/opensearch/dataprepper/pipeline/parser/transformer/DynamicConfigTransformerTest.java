/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.jayway.jsonpath.PathNotFoundException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationFileReader;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationReader;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;
import org.opensearch.dataprepper.pipeline.parser.TestConfigurationProvider;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;

import java.io.File;
import java.io.IOException;
import java.util.Map;


class DynamicConfigTransformerTest {

    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
    private final String RULES_DIRECTORY_PATH = "src/test/resources/transformation/rules";
    private final String TEMPLATES_DIRECTORY_PATH = "src/test/resources/transformation/templates/testSource";

    TransformersFactory transformersFactory;
    RuleEvaluator ruleEvaluator;

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

        transformersFactory = Mockito.spy(new TransformersFactory(RULES_DIRECTORY_PATH,
                TEMPLATES_DIRECTORY_PATH));
        when(transformersFactory.getPluginRuleFileLocation(pluginName)).thenReturn(ruleDocDBFilePath);
        when(transformersFactory.getPluginTemplateFileLocation(pluginName)).thenReturn(templateDocDBFilePath);
        ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(pipelinesDataFlowModel,
                ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration();
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);
        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }

    @Test
    void test_successful_transformation_with_documentdb() throws IOException {

        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCUMENTDB_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCUMENTDB_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCUMENTDB_CONFIG_FILE;
        String expectedDocDBFilePath = TestConfigurationProvider.EXPECTED_TRANSFORMATION_DOCUMENTDB_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        transformersFactory = Mockito.spy(new TransformersFactory(RULES_DIRECTORY_PATH,
                TEMPLATES_DIRECTORY_PATH));
        when(transformersFactory.getPluginRuleFileLocation(pluginName)).thenReturn(ruleDocDBFilePath);
        when(transformersFactory.getPluginTemplateFileLocation(pluginName)).thenReturn(templateDocDBFilePath);
        ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(pipelinesDataFlowModel,
                ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration();
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);
        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }

    @Test
    void test_successful_transformation_with_subpipelines() throws IOException {

        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCUMENTDB_SUBPIPELINES_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCUMENTDB_SUBPIPELINES_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCUMENTDB_CONFIG_FILE;
        String expectedDocDBFilePath = TestConfigurationProvider.EXPECTED_TRANSFORMATION_DOCUMENTDB_SUBPIPLINES_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        transformersFactory = Mockito.spy(new TransformersFactory(RULES_DIRECTORY_PATH,
                TEMPLATES_DIRECTORY_PATH));
        when(transformersFactory.getPluginRuleFileLocation(pluginName)).thenReturn(ruleDocDBFilePath);
        when(transformersFactory.getPluginTemplateFileLocation(pluginName)).thenReturn(templateDocDBFilePath);
        ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(pipelinesDataFlowModel,
                ruleEvaluator);
        PipelinesDataFlowModel transformedModel = transformer.transformConfiguration();
        String transformedYaml = yamlMapper.writeValueAsString(transformedModel);

        Map<String, Object> transformedYamlasMap = yamlMapper.readValue(transformedYaml, Map.class);
        Map<String, Object> expectedYamlasMap = yamlMapper.readValue(new File(expectedDocDBFilePath), Map.class);
        assertThat(expectedYamlasMap).usingRecursiveComparison().isEqualTo(transformedYamlasMap);
    }

    @Test
    void testPathNotFoundInTemplateThrowsException() throws IOException {
        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCDB_SIMPLE_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCDB_SIMPLE_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        transformersFactory = Mockito.spy(new TransformersFactory(RULES_DIRECTORY_PATH,
                TEMPLATES_DIRECTORY_PATH));
        when(transformersFactory.getPluginRuleFileLocation(pluginName)).thenReturn(ruleDocDBFilePath);
        when(transformersFactory.getPluginTemplateFileLocation(pluginName)).thenReturn(templateDocDBFilePath);
        ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelinesDataFlowModel pipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(pipelinesDataFlowModel,
                ruleEvaluator);
        assertThrows(PathNotFoundException.class, () -> transformer.transformConfiguration());
    }
}
