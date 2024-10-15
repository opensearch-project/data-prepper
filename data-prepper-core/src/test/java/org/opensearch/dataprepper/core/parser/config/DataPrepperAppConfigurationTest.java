/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.TestDataProvider;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class DataPrepperAppConfigurationTest {

    private static final DataPrepperAppConfiguration appConfiguration = new DataPrepperAppConfiguration();

    @Test
    void testGivenNoPipelineConfigArgThenResultOfObjectMapperReadValueIsReturned() {
        final FileStructurePathProvider fileStructurePathProvider = mock(FileStructurePathProvider.class);
        final ObjectMapper objectMapper = mock(ObjectMapper.class);

        final DataPrepperConfiguration configuration = appConfiguration.dataPrepperConfiguration(fileStructurePathProvider, objectMapper);

        verify(fileStructurePathProvider)
                .getDataPrepperConfigFileLocation();
        assertThat(configuration, notNullValue());
    }

    @Test
    void testGivenPipelineConfigArgThenResultOfObjectMapperReadValueIsReturned() throws IOException {
        final FileStructurePathProvider fileStructurePathProvider = mock(FileStructurePathProvider.class);
        final ObjectMapper objectMapper = mock(ObjectMapper.class);
        final DataPrepperConfiguration expected = mock(DataPrepperConfiguration.class);

        when(fileStructurePathProvider.getDataPrepperConfigFileLocation())
                .thenReturn(TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE);

        when(objectMapper.readValue(any(File.class), eq(DataPrepperConfiguration.class)))
                .thenReturn(expected);

        final DataPrepperConfiguration configuration = appConfiguration.dataPrepperConfiguration(fileStructurePathProvider, objectMapper);

        assertThat(configuration, is(expected));
    }

    @Test
    void testGivenInvalidPipelineConfigArgThenExceptionThrown() throws IOException {
        final FileStructurePathProvider fileStructurePathProvider = mock(FileStructurePathProvider.class);
        final ObjectMapper objectMapper = mock(ObjectMapper.class);

        when(fileStructurePathProvider.getDataPrepperConfigFileLocation())
                .thenReturn(TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE);

        when(objectMapper.readValue(any(File.class), eq(DataPrepperConfiguration.class)))
                .thenThrow(new IOException());


        assertThrows(IllegalArgumentException.class, () -> appConfiguration.dataPrepperConfiguration(fileStructurePathProvider, objectMapper));
    }

    @Test
    void testPluginModelFromDataPrepperConfigurationAuthentication() {
        final DataPrepperConfiguration configuration = mock(DataPrepperConfiguration.class);

        final PluginModel pluginModel = appConfiguration.authentication(configuration);

        assertThat(pluginModel, is(nullValue()));
        verify(configuration).getAuthentication();
    }

    @Test
    void testGivenReturnAuthenticationThenBeanShouldEqualAuthentication() {
        final DataPrepperConfiguration configuration = mock(DataPrepperConfiguration.class);
        final PluginModel expected = mock(PluginModel.class);

        when(configuration.getAuthentication())
                .thenReturn(expected);

        final PluginModel pluginModel = appConfiguration.authentication(configuration);

        assertThat(pluginModel, is(expected));
        verify(configuration).getAuthentication();
    }
}
