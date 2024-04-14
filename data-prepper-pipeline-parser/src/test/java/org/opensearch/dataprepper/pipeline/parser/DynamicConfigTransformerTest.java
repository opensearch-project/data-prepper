package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.anyString;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.SinkModel;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.transformer.DynamicConfigTransformer;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineConfigurationTransformer;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineTemplateModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DynamicConfigTransformerTest {


    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String RULES_DIRECTORY_PATH = "src/test/resources/transformation/rules";
    private final String TEMPLATES_DIRECTORY_PATH = "src/test/resources/transformation/templates/testSource";

    TransformersFactory transformersFactory;
    RuleEvaluator ruleEvaluator;

    @BeforeEach
    void setUp() {
    }

    @Test
    void testSuccessfulTransformation() throws IOException {

        String docDBUserConfig = TestConfigurationProvider.USER_CONFIG_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String templateDocDBFilePath = TestConfigurationProvider.TEMPLATE_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String ruleDocDBFilePath = TestConfigurationProvider.RULES_TRANSFORMATION_DOCDB1_CONFIG_FILE;
        String pluginName = "documentdb";
        PipelineConfigurationReader pipelineConfigurationReader = new PipelineConfigurationFileReader(docDBUserConfig);
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();

        PipelineTemplateModel templateDataFlowModel = yamlMapper.readValue(new File(templateDocDBFilePath),
                PipelineTemplateModel.class);

        transformersFactory = Mockito.spy(new TransformersFactory(RULES_DIRECTORY_PATH,
                TEMPLATES_DIRECTORY_PATH));
        when(transformersFactory.getPluginRuleFileLocation(pluginName)).thenReturn(ruleDocDBFilePath);
        ruleEvaluator = new RuleEvaluator(transformersFactory);

        // Load the original and template YAML files from the test resources directory
        PipelineConfigurationTransformer transformer = new DynamicConfigTransformer(pipelinesDataflowModelParser,
                ruleEvaluator);
        transformer.transformConfiguration(templateDataFlowModel);

//        String originalSourceYamlFilePath = "src/test/resources/templates/testSource/documentdb1-userconfig.yaml";
//        String expectedSourceYamlFilePath = "src/test/resources/templates/testSource/expectedSourceYaml.yaml";
//
//        String originalYaml = Files.readString(Paths.get(originalSourceYamlFilePath));
//        String templateYaml = Files.readString(Paths.get(templateSourceYamlFilePath));
//        String expectedYaml = Files.readString(Paths.get(expectedSourceYamlFilePath));
//
//        String outputPath = "src/test/resources/templates/testSource/transformedSourceYaml.yaml";

//        String transformedYaml = yamlTransformer.transformYaml(originalYaml, templateYaml);
//
//        FileWriter fileWriter = new FileWriter(outputPath);
//        fileWriter.write(transformedYaml);

//        assertEquals(expectedYaml.trim(), transformedYaml.trim(), "The transformed YAML should match the expected YAML.");
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
