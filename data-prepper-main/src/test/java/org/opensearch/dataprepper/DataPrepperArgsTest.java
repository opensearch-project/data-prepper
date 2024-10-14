/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.DataPrepperArgs;

import java.nio.file.Paths;

import static org.apache.commons.io.FilenameUtils.separatorsToSystem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class DataPrepperArgsTest {
    private static final String PIPELINE_FILE_PATH = "pipeline.yml";
    private static final String DP_CONFIG_YAML_FILE_PATH = "config.yml";
    private static final String LOGSTASH_PIPELINE_FILE_PATH = "src/test/resources/logstash-filter.conf";
    private static final String LOGSTASH_PIPELINE_DIRECTORY_PATH = "src/test/resources/logstash-conf";

    @Test
    void testGivenSingleArgumentThenAssignedToPipelineConfig() {
        final DataPrepperArgs args = new DataPrepperArgs(PIPELINE_FILE_PATH);

        assertThat(args, is(notNullValue()));
        assertThat(args.getPipelineConfigFileLocation(), is(PIPELINE_FILE_PATH));
        assertThat(args.getDataPrepperConfigFileLocation(), is(nullValue()));
    }

    @Test
    void testGivenTwoArgumentThenAssignedCorrectly() {
        final DataPrepperArgs args = new DataPrepperArgs(PIPELINE_FILE_PATH, DP_CONFIG_YAML_FILE_PATH);

        assertThat(args, is(notNullValue()));
        assertThat(args.getPipelineConfigFileLocation(), is(PIPELINE_FILE_PATH));
        assertThat(args.getDataPrepperConfigFileLocation(), is(DP_CONFIG_YAML_FILE_PATH));
    }

    @Test
    void testGivenThreeArgumentThenThrowException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DataPrepperArgs(PIPELINE_FILE_PATH, LOGSTASH_PIPELINE_FILE_PATH, DP_CONFIG_YAML_FILE_PATH));
    }

    @Test
    void testGivenNoArgumentThenThrowException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DataPrepperArgs(new String[]{}));
    }

    @Test
    void testGivenNullArgumentThenThrowException() {
        assertThrows(
                IllegalArgumentException.class,
                DataPrepperArgs::new);
    }

    @Test
    void testGivenLogstashConfigPathThenPipelineConfigCreated() {
        final DataPrepperArgs args = new DataPrepperArgs(LOGSTASH_PIPELINE_FILE_PATH, DP_CONFIG_YAML_FILE_PATH);

        final String configFileEnding = Paths.get("src", "test", "resources", "logstash-filter.yaml").toString();

        assertThat(args, is(notNullValue()));
        assertThat(
                args.getPipelineConfigFileLocation(),
                endsWith(configFileEnding));
        assertThat(args.getDataPrepperConfigFileLocation(), is(DP_CONFIG_YAML_FILE_PATH));
    }

    @Test
    void testGivenLogstashConfigDirectoryThenPipelineConfigCreated() {
        final DataPrepperArgs args = new DataPrepperArgs(LOGSTASH_PIPELINE_DIRECTORY_PATH, DP_CONFIG_YAML_FILE_PATH);

        final String configFile = Paths.get("src", "test", "resources", "logstash-conf/").toString();

        assertThat(args, is(notNullValue()));
        assertThat(separatorsToSystem(args.getPipelineConfigFileLocation()), is(separatorsToSystem(configFile)));
        assertThat(args.getDataPrepperConfigFileLocation(), is(DP_CONFIG_YAML_FILE_PATH));
    }

    @Test
    void testGivenInvalidLogstashConfigPathThenThrowException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DataPrepperArgs("bad-file-path.conf"));
    }
}
