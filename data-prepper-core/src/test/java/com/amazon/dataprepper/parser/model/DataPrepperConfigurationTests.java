/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.amazon.dataprepper.TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE_WITH_TAGS;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PORT_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_CLOUDWATCH_METRICS_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE_WITH_BASIC_AUTHENTICATION;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE_WITH_TAGS;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_MULTIPLE_METRICS_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataPrepperConfigurationTests {
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    private static DataPrepperConfiguration makeConfig(String filePath) throws IOException {
        final File configurationFile = new File(filePath);
        return OBJECT_MAPPER.readValue(configurationFile, DataPrepperConfiguration.class);
    }

    @Test
    public void testParseConfig() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration =
                makeConfig(VALID_DATA_PREPPER_CONFIG_FILE);
        Assert.assertEquals(5678, dataPrepperConfiguration.getServerPort());
    }

    @Test
    public void testSomeDefaultConfig() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration =
                makeConfig(VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE);
        Assert.assertEquals(DataPrepperConfiguration.DEFAULT_CONFIG.getServerPort(), dataPrepperConfiguration.getServerPort());
    }

    @Test
    public void testDefaultMetricsRegistry() {
        final DataPrepperConfiguration dataPrepperConfiguration = DataPrepperConfiguration.DEFAULT_CONFIG;
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes().size(), Matchers.equalTo(1));
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.Prometheus));
    }

    @Test
    public void testCloudWatchMetricsRegistry() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration =
                makeConfig(VALID_DATA_PREPPER_CLOUDWATCH_METRICS_CONFIG_FILE);
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes().size(), Matchers.equalTo(1));
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.CloudWatch));
    }

    @Test
    public void testMultipleMetricsRegistry() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration =
                makeConfig(VALID_DATA_PREPPER_MULTIPLE_METRICS_CONFIG_FILE);
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes().size(), Matchers.equalTo(2));
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.Prometheus));
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.CloudWatch));
    }

    @Test
    void testConfigurationWithHttpBasic() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration =
                makeConfig(VALID_DATA_PREPPER_CONFIG_FILE_WITH_BASIC_AUTHENTICATION);

        assertThat(dataPrepperConfiguration, notNullValue());
        assertThat(dataPrepperConfiguration.getAuthentication(), notNullValue());
        assertThat(dataPrepperConfiguration.getAuthentication().getPluginName(), equalTo("http_basic"));
        assertThat(dataPrepperConfiguration.getAuthentication().getPluginSettings(), notNullValue());
        assertThat(dataPrepperConfiguration.getAuthentication().getPluginSettings(), hasKey("username"));
        assertThat(dataPrepperConfiguration.getAuthentication().getPluginSettings(), hasKey("password"));
    }

    @Test
    void testConfigWithValidMetricTags() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration = makeConfig(
                VALID_DATA_PREPPER_CONFIG_FILE_WITH_TAGS);

        assertThat(dataPrepperConfiguration, notNullValue());
        final Map<String, String> metricTags = dataPrepperConfiguration.getMetricTags();
        assertThat(metricTags, notNullValue());
        assertThat(metricTags.get("testKey1"), equalTo("testValue1"));
        assertThat(metricTags.get("testKey2"), equalTo("testValue2"));
    }

    @Test
    public void testInvalidConfig() {
        assertThrows(UnrecognizedPropertyException.class, () ->
                    makeConfig(INVALID_DATA_PREPPER_CONFIG_FILE));
    }

    @Test
    public void testInvalidPortConfig() {
        assertThrows(ValueInstantiationException.class, () ->
                makeConfig(INVALID_PORT_DATA_PREPPER_CONFIG_FILE));
    }

    @Test
    void testConfigWithInValidMetricTags() {
        assertThrows(ValueInstantiationException.class, () ->
                makeConfig(INVALID_DATA_PREPPER_CONFIG_FILE_WITH_TAGS));
    }
}
