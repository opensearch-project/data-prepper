package org.opensearch.dataprepper.pipeline.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class PipelineConfigurationFileReaderTest {

        @TempDir
        Path tempDir;

        @Test
        void getPipelineConfigurationInputStreams_from_directory_with_no_yaml_files_should_throw() {
            final PipelineConfigurationReader objectUnderTest =
                    new PipelineConfigurationFileReader(TestConfigurationProvider.EMPTY_PIPELINE_DIRECTOTRY);


            final RuntimeException actualException = assertThrows(RuntimeException.class,
                    objectUnderTest::getPipelineConfigurationInputStreams);
            assertThat(actualException.getMessage(), equalTo(
                    String.format("Pipelines configuration file not found at %s", TestConfigurationProvider.EMPTY_PIPELINE_DIRECTOTRY)));
        }

        @Test
        void getPipelineConfigurationInputStreams_with_a_configuration_file_which_does_not_exist_should_throw() {
            final PipelineConfigurationReader objectUnderTest =
                    new PipelineConfigurationFileReader("file_does_not_exist.yml");

            final RuntimeException actualException = assertThrows(RuntimeException.class,
                    objectUnderTest::getPipelineConfigurationInputStreams);
            assertThat(actualException.getMessage(), equalTo("Pipelines configuration file not found at file_does_not_exist.yml"));
        }

        @Test
        void getPipelineConfigurationInput_streams_from_existing_file() throws IOException {

            final String yamlContent = UUID.randomUUID().toString();
            final Path file = tempDir.resolve("test-pipeline.yaml");
            Files.writeString(file, yamlContent);

            final PipelineConfigurationReader objectUnderTest =
                    new PipelineConfigurationFileReader(file.toString());

            final List<InputStream> inputStreams = objectUnderTest.getPipelineConfigurationInputStreams();

            assertThat(inputStreams.size(), equalTo(1));

            try (final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStreams.get(0), StandardCharsets.UTF_8))) {
                final String content = bufferedReader.lines().collect(Collectors.joining(System.lineSeparator()));
                assertThat(content, equalTo(yamlContent));
            }
        }

    @Test
    void getPipelineConfigurationInput_streams_from_existing_directory() throws IOException {


        final String yamlContentPipelineOne = UUID.randomUUID().toString();
        final String yamlContentPipelineTwo = UUID.randomUUID().toString();

        Files.writeString(tempDir.resolve("test-pipeline-1.yaml"), yamlContentPipelineOne);
        Files.writeString(tempDir.resolve("tset-pipeline-2.yml"), yamlContentPipelineTwo);

        final PipelineConfigurationReader objectUnderTest =
                new PipelineConfigurationFileReader(tempDir.toString());

        final List<InputStream> inputStreams = objectUnderTest.getPipelineConfigurationInputStreams();

        assertThat(inputStreams.size(), equalTo(2));

        try (final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStreams.get(0), StandardCharsets.UTF_8))) {
            final String content = bufferedReader.lines().collect(Collectors.joining(System.lineSeparator()));
            assertThat(content, equalTo(yamlContentPipelineOne));
        }

        try (final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStreams.get(1), StandardCharsets.UTF_8))) {
            final String content = bufferedReader.lines().collect(Collectors.joining(System.lineSeparator()));
            assertThat(content, equalTo(yamlContentPipelineTwo));
        }
    }
}
