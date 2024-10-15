/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core;

import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataPrepperArgumentConfigurationTest {
    private static final String PIPELINE_CONFIG_FILE_PATH = "~/.config/data-prepper/pipeline.yaml";
    private static final String DATA_PREPPER_CONFIG_FILE_PATH = "~/.config/data-prepper/data-prepper-config.yaml";

    private DataPrepperArgumentConfiguration createObjectUnderTest() {
        return new DataPrepperArgumentConfiguration();
    }

    @Test
    void testGivenValidCommandLineArgumentThenDataPrepperArgsBeanCreated() {
        final Environment env = mock(Environment.class);

        when(env.getProperty("nonOptionArgs"))
                .thenReturn(PIPELINE_CONFIG_FILE_PATH);

        final DataPrepperArgs args = createObjectUnderTest().dataPrepperArgs(env);

        assertThat(args.getPipelineConfigFileLocation(), is(PIPELINE_CONFIG_FILE_PATH));
        assertThat(args.getDataPrepperConfigFileLocation(), is(nullValue()));
    }

    @Test
    void testGivenValidCommandLineArgumentsThenDataPrepperArgsBeanCreated() {
        final Environment env = mock(Environment.class);

        when(env.getProperty("nonOptionArgs"))
                .thenReturn(PIPELINE_CONFIG_FILE_PATH + "," + DATA_PREPPER_CONFIG_FILE_PATH);

        final DataPrepperArgs args = createObjectUnderTest().dataPrepperArgs(env);

        assertThat(args.getPipelineConfigFileLocation(), is(PIPELINE_CONFIG_FILE_PATH));
        assertThat(args.getDataPrepperConfigFileLocation(), is(DATA_PREPPER_CONFIG_FILE_PATH));
    }

    @Test
    void testGivenNoCommandLineArgumentsThenExceptionThrown() {
        final Environment env = mock(Environment.class);

        final DataPrepperArgumentConfiguration objectUnderTest = createObjectUnderTest();
        assertThrows(
                RuntimeException.class,
                () -> objectUnderTest.dataPrepperArgs(env));
    }
}