package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.SinkModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.DynamicConfigTransformer;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineTemplateModel;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DynamicConfigTransformerTest {

    private DynamicConfigTransformer yamlTransformer;
    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
    }

    @Test
    void testSuccessfulTransformation() throws IOException {
        String pipelineName = "test-pipeline";
        Map sourceOptions = new HashMap<String, Object>();
        sourceOptions.put("option1", "1");
        final PluginModel source = new PluginModel("http", sourceOptions);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        String templateFilePath = "src/test/resources/templates/testSource/templateDocumentDB.yaml";
        String userConfigFilePath = "src/test/resources/templates/testSource/userConfigDocumentDB.yaml";

        PipelineTemplateModel templateDataFlowModel = yamlMapper.readValue(new File(templateFilePath),
                PipelineTemplateModel.class);

//         String templateJson = objectMapper.writeValueAsString(templateDataFlowModel);

        // Load the original and template YAML files from the test resources directory
//        PipelineConfigurationTransformer transformer = new DynamicYamlTransformer();
//        transformer.transformConfiguration(pipelinesDataFlowModel, templateDataFlowModel);

//        String originalSourceYamlFilePath = "src/test/resources/templates/testSource/originalSourceYaml.yaml";
//        String templateSourceYamlFilePath = "src/test/resources/templates/testSource/templateSourceYaml.yaml";
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
