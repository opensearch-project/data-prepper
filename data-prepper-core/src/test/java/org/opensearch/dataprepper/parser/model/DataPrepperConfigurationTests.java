/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model;

import org.opensearch.dataprepper.TestDataProvider;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.parser.DataPrepperDurationDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataPrepperConfigurationTests {
    private static SimpleModule simpleModule = new SimpleModule().addDeserializer(Duration.class, new DataPrepperDurationDeserializer());
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory()).registerModule(simpleModule);

    private static DataPrepperConfiguration makeConfig(String filePath) throws IOException {
        final File configurationFile = new File(filePath);
        return OBJECT_MAPPER.readValue(configurationFile, DataPrepperConfiguration.class);
    }

    @Test
    public void testParseConfig() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration =
                makeConfig(TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE);
        Assert.assertEquals(5678, dataPrepperConfiguration.getServerPort());
    }

    @Test
    public void testSomeDefaultConfig() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration =
                makeConfig(TestDataProvider.VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE);
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
                makeConfig(TestDataProvider.VALID_DATA_PREPPER_CLOUDWATCH_METRICS_CONFIG_FILE);
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes().size(), Matchers.equalTo(1));
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.CloudWatch));
    }

    @Test
    public void testMultipleMetricsRegistry() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration =
                makeConfig(TestDataProvider.VALID_DATA_PREPPER_MULTIPLE_METRICS_CONFIG_FILE);
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes().size(), Matchers.equalTo(2));
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.Prometheus));
        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.CloudWatch));
    }

    @Test
    void testConfigurationWithHttpBasic() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration =
                makeConfig(TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE_WITH_BASIC_AUTHENTICATION);

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
                TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE_WITH_TAGS);

        assertThat(dataPrepperConfiguration, notNullValue());
        final Map<String, String> metricTags = dataPrepperConfiguration.getMetricTags();
        assertThat(metricTags, notNullValue());
        assertThat(metricTags.get("testKey1"), equalTo("testValue1"));
        assertThat(metricTags.get("testKey2"), equalTo("testValue2"));
    }

    @Test
    void testConfigWithValidProcessorShutdownTimeout() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration = makeConfig(
                TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE_WITH_PROCESSOR_SHUTDOWN_TIMEOUT);

        assertThat(dataPrepperConfiguration, notNullValue());
        final Duration processorShutdownTimeout = dataPrepperConfiguration.getProcessorShutdownTimeout();
        assertThat(processorShutdownTimeout, notNullValue());
        assertThat(processorShutdownTimeout, equalTo(Duration.ofSeconds(1)));
    }

    @Test
    void testConfigWithValidSinkShutdownTimeout() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration = makeConfig(
                TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE_WITH_SINK_SHUTDOWN_TIMEOUT);

        assertThat(dataPrepperConfiguration, notNullValue());
        final Duration sinkShutdownTimeout = dataPrepperConfiguration.getSinkShutdownTimeout();
        assertThat(sinkShutdownTimeout, notNullValue());
        assertThat(sinkShutdownTimeout, equalTo(Duration.ofSeconds(1)));
    }

    @Test
    void testPeerForwarderConfig() throws IOException {
        final DataPrepperConfiguration dataPrepperConfiguration = makeConfig(TestDataProvider.VALID_PEER_FORWARDER_DATA_PREPPER_CONFIG_FILE);

        assertThat(dataPrepperConfiguration.getPeerForwarderConfiguration(), isA(PeerForwarderConfiguration.class));
    }

    @Test
    public void testInvalidConfig() {
        assertThrows(UnrecognizedPropertyException.class, () ->
                    makeConfig(TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE));
    }

    @Test
    public void testInvalidPortConfig() {
        assertThrows(ValueInstantiationException.class, () ->
                makeConfig(TestDataProvider.INVALID_PORT_DATA_PREPPER_CONFIG_FILE));
    }

    @Test
    void testConfigWithInValidMetricTags() {
        assertThrows(ValueInstantiationException.class, () ->
                makeConfig(TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE_WITH_TAGS));
    }

    @Test
    void testConfigWithInValidProcessorShutdownTimeout() {
        assertThrows(JsonMappingException.class, () ->
                makeConfig(TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE_WITH_BAD_PROCESSOR_SHUTDOWN_TIMEOUT));
    }

    @Test
    void testConfigWithInValidSinkShutdownTimeout() {
        assertThrows(JsonMappingException.class, () ->
                makeConfig(TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE_WITH_BAD_SINK_SHUTDOWN_TIMEOUT));
    }

    @Test
    void testConfigWithNegativeProcessorShutdownTimeout() {
        assertThrows(ValueInstantiationException.class, () ->
                makeConfig(TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE_WITH_NEGATIVE_PROCESSOR_SHUTDOWN_TIMEOUT));
    }

    @Test
    void testConfigWithNegativeSinkShutdownTimeout() {
        assertThrows(ValueInstantiationException.class, () ->
                makeConfig(TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE_WITH_NEGATIVE_SINK_SHUTDOWN_TIMEOUT));
    }
}
