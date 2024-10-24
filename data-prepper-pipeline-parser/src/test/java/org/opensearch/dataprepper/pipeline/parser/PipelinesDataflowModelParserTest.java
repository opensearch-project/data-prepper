/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelinesDataflowModelParserTest {

    @Mock
    private PipelineConfigurationReader pipelineConfigurationReader;

    @Test
    void parseConfiguration_with_multiple_valid_pipelines() throws FileNotFoundException {
        when(pipelineConfigurationReader.getPipelineConfigurationInputStreams())
                .thenReturn(List.of(new FileInputStream(TestConfigurationProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE)));

        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        assertThat(actualPipelinesDataFlowModel.getPipelines().keySet(),
                equalTo(TestConfigurationProvider.VALID_MULTIPLE_PIPELINE_NAMES));
    }

    @Test
    void parseConfiguration_from_string_input_stream_creates_the_correct_model() {

        final String configurationYamlString = "# this configuration file is solely for testing formatting\n" +
                "test-pipeline-1:\n" +
                "  source:\n" +
                "    file:\n" +
                "      path: \"/tmp/file-source.tmp\"\n" +
                "  buffer:\n" +
                "    bounded_blocking: #to check non object nodes for plugins\n" +
                "  sink:\n" +
                "    - pipeline:\n" +
                "       name: \"test-pipeline-2\"\n" +
                "test-pipeline-2:\n" +
                "  source:\n" +
                "    pipeline:\n" +
                "      name: \"test-pipeline-1\"\n" +
                "  sink:\n" +
                "    - pipeline:\n" +
                "       name: \"test-pipeline-3\"\n" +
                "test-pipeline-3:\n" +
                "  source:\n" +
                "    pipeline:\n" +
                "      name: \"test-pipeline-2\"\n" +
                "  sink:\n" +
                "    - file:\n" +
                "       path: \"/tmp/todelete.txt\"";

        when(pipelineConfigurationReader.getPipelineConfigurationInputStreams())
                .thenReturn(List.of(new ByteArrayInputStream(configurationYamlString.getBytes())));

        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        assertThat(actualPipelinesDataFlowModel.getPipelines().keySet(),
                equalTo(TestConfigurationProvider.VALID_MULTIPLE_PIPELINE_NAMES));
    }

    @ParameterizedTest
    @ValueSource(strings = {TestConfigurationProvider.VALID_PIPELINE_CONFIG_FILE_WITH_DEPRECATED_EXTENSIONS,
            TestConfigurationProvider.VALID_PIPELINE_CONFIG_FILE_WITH_EXTENSION})
    void parseConfiguration_with_valid_pipelines_and_extension(final String filePath) throws FileNotFoundException {
        when(pipelineConfigurationReader.getPipelineConfigurationInputStreams())
                .thenReturn(List.of(new FileInputStream(filePath)));

        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        assertThat(actualPipelinesDataFlowModel.getPipelines().keySet(),
                equalTo(TestConfigurationProvider.VALID_MULTIPLE_PIPELINE_NAMES));
        assertThat(actualPipelinesDataFlowModel.getPipelineExtensions(), notNullValue());
        assertThat(actualPipelinesDataFlowModel.getPipelineExtensions().getExtensionMap(), equalTo(
                Map.of("test_extension", Map.of("test_attribute", "test_string_1"))
        ));
    }

    @Test
    void parseConfiguration_with_incompatible_version_should_throw() throws FileNotFoundException {
        when(pipelineConfigurationReader.getPipelineConfigurationInputStreams())
                .thenReturn(List.of(new FileInputStream(TestConfigurationProvider.INCOMPATIBLE_VERSION_CONFIG_FILE)));

        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);

        final RuntimeException actualException = assertThrows(
                RuntimeException.class, pipelinesDataflowModelParser::parseConfiguration);
        assertThat(actualException.getMessage(),
                equalTo(String.format("The version: 3005.0 is not compatible with the current version: %s", DataPrepperVersion.getCurrentVersion())));
    }

    @Test
    void parseConfiguration_from_directory_with_multiple_files_creates_the_correct_model() throws Exception {
        final File directoryLocation = new File(TestConfigurationProvider.MULTI_FILE_PIPELINE_DIRECTOTRY);
        final List<InputStream> fileInputStreams = Stream.of(directoryLocation.listFiles())
                .map(file -> {
                    try {
                        return new FileInputStream(file);
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());

        when(pipelineConfigurationReader.getPipelineConfigurationInputStreams()).thenReturn(fileInputStreams);

        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        assertThat(actualPipelinesDataFlowModel.getPipelines().keySet(),
                equalTo(TestConfigurationProvider.VALID_MULTIPLE_PIPELINE_NAMES));
        assertThat(actualPipelinesDataFlowModel.getDataPrepperVersion(), nullValue());
    }

    @Test
    void parseConfiguration_from_directory_with_multiple_files_and_pipeline_extensions_should_throw() {
        final File directoryLocation = new File(TestConfigurationProvider.MULTI_FILE_PIPELINE_WITH_DISTRIBUTED_PIPELINE_CONFIGURATIONS_DIRECTOTRY);
        final List<InputStream> fileInputStreams = Stream.of(directoryLocation.listFiles())
                .map(file -> {
                    try {
                        return new FileInputStream(file);
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());

        when(pipelineConfigurationReader.getPipelineConfigurationInputStreams()).thenReturn(fileInputStreams);

        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);
        final ParseException actualException = assertThrows(
                ParseException.class, pipelinesDataflowModelParser::parseConfiguration);
        assertThat(actualException.getMessage(), equalTo(
                "extension/pipeline_configurations and definition must all be defined in a single YAML file " +
                        "if extension/pipeline_configurations is configured."));
    }

    @Test
    void parseConfiguration_from_directory_with_single_file_creates_the_correct_model() {
        final File directoryLocation = new File(TestConfigurationProvider.SINGLE_FILE_PIPELINE_DIRECTOTRY);
        final List<InputStream> fileInputStreams = Stream.of(directoryLocation.listFiles())
                .map(file -> {
                    try {
                        return new FileInputStream(file);
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());

        when(pipelineConfigurationReader.getPipelineConfigurationInputStreams()).thenReturn(fileInputStreams);

        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        assertThat(actualPipelinesDataFlowModel.getPipelines().keySet(),
                equalTo(TestConfigurationProvider.VALID_MULTIPLE_PIPELINE_NAMES));
    }

    @Test
    void parseConfiguration_check_successful_transformation() {
        final File directoryLocation = new File(TestConfigurationProvider.SINGLE_FILE_PIPELINE_DIRECTOTRY);
        final List<InputStream> fileInputStreams = Stream.of(directoryLocation.listFiles())
                .map(file -> {
                    try {
                        return new FileInputStream(file);
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());

        when(pipelineConfigurationReader.getPipelineConfigurationInputStreams()).thenReturn(fileInputStreams);

        final PipelinesDataflowModelParser pipelinesDataflowModelParser =
                new PipelinesDataflowModelParser(pipelineConfigurationReader);
        final PipelinesDataFlowModel actualPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
        assertThat(actualPipelinesDataFlowModel.getPipelines().keySet(),
                equalTo(TestConfigurationProvider.VALID_MULTIPLE_PIPELINE_NAMES));
    }
}