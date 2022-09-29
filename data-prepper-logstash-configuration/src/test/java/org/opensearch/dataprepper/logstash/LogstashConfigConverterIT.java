/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash;

import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;

/**
 * Integration tests for the Logstash Configuration Converter.
 * <p>
 * These tests use all the <code>.conf</code> files from the
 * <code>src/test/resources/org/opensearch/dataprepper/logstash</code> directory.
 * Each file will be tested by converting and then validating that it can deserialize
 * into a {@link PipelinesDataFlowModel} instance. Additionally, any files with a
 * matching <code>.expected.yaml</code> file will be compared against that YAML file.
 * <p>
 * You can add more test cases by adding both a <code>.conf</code> file and a
 * <code>.expected.yaml</code> file. If the mapping is non-deterministic, you can forego
 * adding the <code>.expected.yaml</code> file.
 */
public class LogstashConfigConverterIT {

    private static final String OUTPUT_DIRECTORY = "build/resources/test/org/opensearch/dataprepper/logstash/";

    private LogstashConfigConverter createObjectUnderTest() {
        return new LogstashConfigConverter();
    }

    @ParameterizedTest
    @ArgumentsSource(LogstashToYamlPathsProviders.class)
    void convertLogstashConfigurationToPipeline_should_return_converted_file_with_the_expected_YAML(final String configurationPath, final String expectedYamlPath) throws IOException {
        final String actualPath = createObjectUnderTest().convertLogstashConfigurationToPipeline(configurationPath, OUTPUT_DIRECTORY);

        assertThat(actualPath, notNullValue());

        final String dataPrepperConfigurationString = Files.readString(Path.of(actualPath));
        final String expectedDataPrepperConfigurationString = Files.readString(Path.of(expectedYamlPath));

        assertThat(dataPrepperConfigurationString, equalTo(expectedDataPrepperConfigurationString));
    }

    @ParameterizedTest
    @ArgumentsSource(LogstashPathsProviders.class)
    void convertLogstashConfigurationToPipeline_should_return_valid_PipelinesDataFlowModel_with_the_single_known_pipeline(final String configurationPath) throws IOException {
        final String actualPath = createObjectUnderTest().convertLogstashConfigurationToPipeline(configurationPath, OUTPUT_DIRECTORY);

        assertThat(actualPath, notNullValue());

        final String dataPrepperConfigurationString = Files.readString(Path.of(actualPath));

        assertThat(dataPrepperConfigurationString, notNullValue());

        final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        final PipelinesDataFlowModel pipelinesDataFlowModel = objectMapper.readValue(dataPrepperConfigurationString, PipelinesDataFlowModel.class);
        assertThat(pipelinesDataFlowModel, notNullValue());
        assertThat(pipelinesDataFlowModel.getPipelines(), notNullValue());
        assertThat(pipelinesDataFlowModel.getPipelines().size(), equalTo(1));
        assertThat(pipelinesDataFlowModel.getPipelines(), hasKey("logstash-converted-pipeline"));
        assertThat(pipelinesDataFlowModel.getPipelines().get("logstash-converted-pipeline"), notNullValue());
    }


    /**
     * Provides arguments for a Logstash configuration path. This will include
     * all Logstash configuration files, whether they have an expected YAML or not.
     */
    static class LogstashPathsProviders implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return provideLogstashConfigurationFiles()
                    .map(Arguments::of);
        }
    }

    /**
     * Provides JUnit5 arguments for Logstash configuration paths along with the
     * expected YAML path.
     */
    static class LogstashToYamlPathsProviders implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return provideLogstashConfigurationFiles()
                    .filter(logstashPath -> Files.exists(Paths.get(getExpectedFileName(logstashPath))))
                    .map(logstashPath -> Arguments.of(logstashPath, getExpectedFileName(logstashPath)));
        }

        private String getExpectedFileName(final String file) {
            return file.replace(".conf", ".expected.yaml");
        }
    }

    private static Stream<String> provideLogstashConfigurationFiles() {
        final File directoryPath = new File("src/test/resources/org/opensearch/dataprepper/logstash");

        return Arrays.stream(Objects.requireNonNull(directoryPath.listFiles((dir, name) -> name.endsWith(".conf"))))
                .filter(File::isFile)
                .map(File::getPath);
    }
}
