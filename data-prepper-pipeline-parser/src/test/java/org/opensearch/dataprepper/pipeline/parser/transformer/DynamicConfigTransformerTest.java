/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
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
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String RULES_DIRECTORY_PATH = "src/test/resources/transformation/rules";
    private final String TEMPLATES_DIRECTORY_PATH = "src/test/resources/transformation/templates/testSource";

    TransformersFactory transformersFactory;
    RuleEvaluator ruleEvaluator;

    @BeforeEach
    void setUp() {
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
        assertEquals(expectedYamlasMap, transformedYamlasMap, "The transformed YAML should match the expected YAML.");
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

//        PipelineTemplateModel templateDataFlowModel = yamlMapper.readValue(new File(templateDocDBFilePath),
//                PipelineTemplateModel.class);

        transformersFactory = Mockito.spy(new TransformersFactory(RULES_DIRECTORY_PATH,
                TEMPLATES_DIRECTORY_PATH));
        when(transformersFactory.getPluginRuleFileLocation(pluginName)).thenReturn(ruleDocDBFilePath);
        ruleEvaluator = new RuleEvaluator(transformersFactory);
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
        assertEquals(expectedYamlasMap, transformedYamlasMap, "The transformed YAML should match the expected YAML.");
    }


    @Test
    void testPathNotFoundInTemplate() throws IOException {

        String templateYaml = "dodb-pipeline:\n" +
                "  source:\n" +
                "    documentdb:\n" +
                "      hostname: \"{{dodb-pipeline.source.documentdb.hostname}}\"\n";

        String expectedYaml = "dodb-pipeline:\n" +
                "  source:\n" +
                "    documentdb:\n" +
                "      hostname: \"database.example.com\"\n";

//        String transformedYaml = yamlTransformer.transformYaml(originalYaml, templateYaml);

//        assertEquals(expectedYaml.trim(), transformedYaml.trim(), "The transformed YAML should not include unspecified paths.");
    }

    @Test
    void testIOExceptionHandling() {
        String invalidYaml = "dodb-pipeline: [";
//        assertThrows(RuntimeException.class, () -> {
//            yamlTransformer.transformYaml(invalidYaml, invalidYaml);
//        }, "A RuntimeException should be thrown when an IOException occurs.");
    }


}
