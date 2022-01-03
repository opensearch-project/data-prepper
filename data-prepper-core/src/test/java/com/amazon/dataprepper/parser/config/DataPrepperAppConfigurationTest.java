package com.amazon.dataprepper.parser.config;

import com.amazon.dataprepper.TestDataProvider;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class DataPrepperAppConfigurationTest {

    private static final DataPrepperAppConfiguration appConfiguration = new DataPrepperAppConfiguration();
    private static final String pipelineConfigFilePath = "~/.config/data-prepper/pipeline.yaml";
    private static final String dataPrepperConfigFilePath = "~/.config/data-prepper/data-prepper-config.yaml";

    @Test
    public void testGivenValidCommandLineArgumentThenDataPrepperArgsBeanCreated() {
        Environment env = mock(Environment.class);

        when(env.getProperty("nonOptionArgs"))
                .thenReturn(pipelineConfigFilePath);

        DataPrepperArgs args = appConfiguration.dataPrepperArgs(env);

        assertThat(args.getPipelineConfigFileLocation(), is(pipelineConfigFilePath));
        assertThat(args.getDataPrepperConfigFileLocation(), is(nullValue()));
    }

    @Test
    public void testGivenValidCommandLineArgumentsThenDataPrepperArgsBeanCreated() {
        Environment env = mock(Environment.class);

        when(env.getProperty("nonOptionArgs"))
                .thenReturn(pipelineConfigFilePath + "," + dataPrepperConfigFilePath);

        DataPrepperArgs args = appConfiguration.dataPrepperArgs(env);

        assertThat(args.getPipelineConfigFileLocation(), is(pipelineConfigFilePath));
        assertThat(args.getDataPrepperConfigFileLocation(), is(dataPrepperConfigFilePath));
    }

    @Test
    public void testGivenNoCommandLineArgumentsThenExceptionThrown() {
        Environment env = mock(Environment.class);

        assertThrows(
                RuntimeException.class,
                () -> appConfiguration.dataPrepperArgs(env));
    }


    @Test
    public void testGivenNoPipelineConfigArgThenResultOfObjectMapperReadValueIsReturned() throws IOException {
        DataPrepperArgs dataPrepperArgs = mock(DataPrepperArgs.class);
        ObjectMapper objectMapper = mock(ObjectMapper.class);

        DataPrepperConfiguration configuration = appConfiguration.dataPrepperConfiguration(dataPrepperArgs, objectMapper);

        verify(dataPrepperArgs, times(1))
                .getDataPrepperConfigFileLocation();
        assertThat(configuration, notNullValue());
    }

    @Test
    public void testGivenPipelineConfigArgThenResultOfObjectMapperReadValueIsReturned() throws IOException {
        DataPrepperArgs dataPrepperArgs = mock(DataPrepperArgs.class);
        ObjectMapper objectMapper = mock(ObjectMapper.class);
        DataPrepperConfiguration expected = mock(DataPrepperConfiguration.class);

        when(dataPrepperArgs.getDataPrepperConfigFileLocation())
                .thenReturn(TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE);

        when(objectMapper.readValue(any(File.class), eq(DataPrepperConfiguration.class)))
                .thenReturn(expected);

        DataPrepperConfiguration configuration = appConfiguration.dataPrepperConfiguration(dataPrepperArgs, objectMapper);

        assertThat(configuration, is(expected));
    }

    @Test
    public void testGivenInvalidPipelineConfigArgThenExceptionThrown() throws IOException {
        DataPrepperArgs dataPrepperArgs = mock(DataPrepperArgs.class);
        ObjectMapper objectMapper = mock(ObjectMapper.class);

        when(dataPrepperArgs.getDataPrepperConfigFileLocation())
                .thenReturn(TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE);

        when(objectMapper.readValue(any(File.class), eq(DataPrepperConfiguration.class)))
                .thenThrow(new IOException());


        assertThrows(IllegalArgumentException.class, () -> appConfiguration.dataPrepperConfiguration(dataPrepperArgs, objectMapper));
    }
}