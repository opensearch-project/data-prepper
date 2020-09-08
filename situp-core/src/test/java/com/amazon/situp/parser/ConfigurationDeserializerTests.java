package com.amazon.situp.parser;

import com.amazon.situp.parser.model.PipelineConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.Test;

import static com.amazon.situp.TestDataProvider.MISSING_COMPONENT_CONFIGURATION_FILE;
import static com.amazon.situp.TestDataProvider.NULL_NAME_CONFIGURATION_FILE;
import static com.amazon.situp.TestDataProvider.VALID_CONFIGURATION_FILE;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithEmptyConfigurations;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithNoPluginsForSource;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithNullPipelineName;
import static com.amazon.situp.TestDataProvider.readConfigurationFileContent;
import static com.amazon.situp.TestDataProvider.validPipelineConfiguration;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

public class ConfigurationDeserializerTests {

    private static final ObjectMapper SIMPLE_OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testEmptyConfigurationFile() {
        readFileAndAssertActualWithExpected(null,
                pipelineConfigurationWithEmptyConfigurations());
    }

    @Test
    public void testMissingComponentConfigurationFile() {
        readFileAndAssertActualWithExpected(MISSING_COMPONENT_CONFIGURATION_FILE,
                pipelineConfigurationWithNoPluginsForSource());
    }

    @Test
    public void testDeserializeOfValidPipelineConfigurationFile() {
        readFileAndAssertActualWithExpected(VALID_CONFIGURATION_FILE,
                validPipelineConfiguration());
    }

    @Test
    public void testDeserializeOfNullNamePipelineConfigurationFile() {
        readFileAndAssertActualWithExpected(NULL_NAME_CONFIGURATION_FILE,
                pipelineConfigurationWithNullPipelineName());
    }

    private void readFileAndAssertActualWithExpected(
            final String configurationFile,
            final PipelineConfiguration expectedConfiguration) {
        try {
            final PipelineConfiguration actualConfiguration = SIMPLE_OBJECT_MAPPER.readValue(
                    readConfigurationFileContent(configurationFile), PipelineConfiguration.class);
            assertThat(actualConfiguration, Matchers.is(expectedConfiguration));
        } catch (Exception ex) {
            assertFalse("Encountered exception during execution of test",false);
        }
    }

}
