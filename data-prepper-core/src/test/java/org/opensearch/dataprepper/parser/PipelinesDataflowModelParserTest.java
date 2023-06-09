package org.opensearch.dataprepper.parser;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.TestDataProvider;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelinesDataflowModelParserTest {
    @Test
    void parseConfiguration_with_multiple_valid_pipelines() {
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(TestDataProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        assertThat(actualPipelinesDataFlowModel.getPipelineExtensions(), nullValue());
        assertThat(actualPipelinesDataFlowModel.getPipelines().keySet(),
                equalTo(TestDataProvider.VALID_MULTIPLE_PIPELINE_NAMES));
    }

    @Test
    void parseConfiguration_with_pipeline_extensions() {
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(TestDataProvider.VALID_PIPELINE_WITH_TEST_EXTENSION_FILE);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        assertThat(actualPipelinesDataFlowModel.getPipelines().keySet().size(), equalTo(1));
        assertThat(actualPipelinesDataFlowModel.getPipelineExtensions(), notNullValue());
        final Map<String, Object> extensionMap = actualPipelinesDataFlowModel.getPipelineExtensions().getExtensionMap();
        assertThat(extensionMap.containsKey("test_extension"), is(true));
        assertThat(extensionMap.get("test_extension"), equalTo(Map.of("test_attribute", "test_string")));
    }

    @Test
    void parseConfiguration_with_incompatible_version_should_throw() {
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(TestDataProvider.INCOMPATIBLE_VERSION_CONFIG_FILE);

        final RuntimeException actualException = assertThrows(
                RuntimeException.class, pipelinesDataflowModelParser::parseConfiguration);
        assertThat(actualException.getMessage(),
                equalTo(String.format("The version: 3005.0 is not compatible with the current version: %s", DataPrepperVersion.getCurrentVersion())));
    }

    @Test
    void parseConfiguration_with_a_configuration_file_which_does_not_exist_should_throw() {
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser("file_does_no_exist.yml");
        final RuntimeException actualException = assertThrows(RuntimeException.class,
                pipelinesDataflowModelParser::parseConfiguration);
        assertThat(actualException.getMessage(), equalTo("Pipelines configuration file not found at file_does_no_exist.yml"));
    }

    @Test
    void parseConfiguration_from_directory_with_multiple_files_creates_the_correct_model() {
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(TestDataProvider.MULTI_FILE_PIPELINE_DIRECTOTRY);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        assertThat(actualPipelinesDataFlowModel.getPipelines().keySet(),
                equalTo(TestDataProvider.VALID_MULTIPLE_PIPELINE_NAMES));
    }

    @Test
    void parseConfiguration_from_directory_with_single_file_creates_the_correct_model() {
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(TestDataProvider.SINGLE_FILE_PIPELINE_DIRECTOTRY);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        assertThat(actualPipelinesDataFlowModel.getPipelines().keySet(),
                equalTo(TestDataProvider.VALID_MULTIPLE_PIPELINE_NAMES));
    }

    @Test
    void parseConfiguration_from_directory_with_no_yaml_files_should_throw() {
        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(TestDataProvider.EMPTY_PIPELINE_DIRECTOTRY);
        final RuntimeException actualException = assertThrows(RuntimeException.class,
                pipelinesDataflowModelParser::parseConfiguration);
        assertThat(actualException.getMessage(), equalTo(
                String.format("Pipelines configuration file not found at %s", TestDataProvider.EMPTY_PIPELINE_DIRECTOTRY)));
    }
}