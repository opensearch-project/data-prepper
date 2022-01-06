/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

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

    @Test
    public void testGivenSingleArgumentThenAssignedToPipelineConfig() {
        DataPrepperArgs args = new DataPrepperArgs(PIPELINE_FILE_PATH);

        assertThat(args, is(notNullValue()));
        assertThat(args.getPipelineConfigFileLocation(), is(PIPELINE_FILE_PATH));
        assertThat(args.getDataPrepperConfigFileLocation(), is(nullValue()));
    }

    @Test
    public void testGivenTwoArgumentThenAssignedCorrectly() {
        DataPrepperArgs args = new DataPrepperArgs(PIPELINE_FILE_PATH, DP_CONFIG_YAML_FILE_PATH);

        assertThat(args, is(notNullValue()));
        assertThat(args.getPipelineConfigFileLocation(), is(PIPELINE_FILE_PATH));
        assertThat(args.getDataPrepperConfigFileLocation(), is(DP_CONFIG_YAML_FILE_PATH));
    }

    @Test
    public void testGivenThreeArgumentThenThrowException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DataPrepperArgs(PIPELINE_FILE_PATH, LOGSTASH_PIPELINE_FILE_PATH, DP_CONFIG_YAML_FILE_PATH));
    }

    @Test
    public void testGivenNoArgumentThenThrowException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DataPrepperArgs());
    }

    @Test
    public void testGivenLogstashConfigPathThenPipelineConfigCreated() {
        DataPrepperArgs args = new DataPrepperArgs(LOGSTASH_PIPELINE_FILE_PATH, DP_CONFIG_YAML_FILE_PATH);

        assertThat(args, is(notNullValue()));
        assertThat(
                args.getPipelineConfigFileLocation(),
                endsWith("src/test/resources/logstash-filter.yaml"));
        assertThat(args.getDataPrepperConfigFileLocation(), is(DP_CONFIG_YAML_FILE_PATH));
    }

    @Test
    public void testGivenInvalidLogstashConfigPathThenThrowException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DataPrepperArgs("bad-file-path.conf"));
    }
}