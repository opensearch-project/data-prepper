/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.parser.model;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.File;

import static com.amazon.dataprepper.TestDataProvider.INVALID_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PORT_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_CLOUDWATCH_METRICS_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE_WITH_BASIC_AUTHENTICATION;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_MULTIPLE_METRICS_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataPrepperConfigurationTests {

//    @Test
//    public void testParseConfig() {
//        final DataPrepperConfiguration dataPrepperConfiguration =
//                DataPrepperConfiguration.fromFile(new File(VALID_DATA_PREPPER_CONFIG_FILE));
//        Assert.assertEquals(5678, dataPrepperConfiguration.getServerPort());
//    }
//
//    @Test
//    public void testSomeDefaultConfig() {
//        final DataPrepperConfiguration dataPrepperConfiguration =
//                DataPrepperConfiguration.fromFile(new File(VALID_DATA_PREPPER_SOME_DEFAULT_CONFIG_FILE));
//        Assert.assertEquals(DataPrepperConfiguration.DEFAULT_CONFIG.getServerPort(), dataPrepperConfiguration.getServerPort());
//    }
//
//    @Test
//    public void testDefaultMetricsRegistry() {
//        final DataPrepperConfiguration dataPrepperConfiguration = DataPrepperConfiguration.DEFAULT_CONFIG;
//        assertThat(dataPrepperConfiguration.getMetricRegistryTypes().size(), Matchers.equalTo(1));
//        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.Prometheus));
//    }
//
//    @Test
//    public void testCloudWatchMetricsRegistry() {
//        final DataPrepperConfiguration dataPrepperConfiguration =
//                DataPrepperConfiguration.fromFile(new File(VALID_DATA_PREPPER_CLOUDWATCH_METRICS_CONFIG_FILE));
//        assertThat(dataPrepperConfiguration.getMetricRegistryTypes().size(), Matchers.equalTo(1));
//        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.CloudWatch));
//    }
//
//    @Test
//    public void testMultipleMetricsRegistry() {
//        final DataPrepperConfiguration dataPrepperConfiguration =
//                DataPrepperConfiguration.fromFile(new File(VALID_DATA_PREPPER_MULTIPLE_METRICS_CONFIG_FILE));
//        assertThat(dataPrepperConfiguration.getMetricRegistryTypes().size(), Matchers.equalTo(2));
//        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.Prometheus));
//        assertThat(dataPrepperConfiguration.getMetricRegistryTypes(), Matchers.hasItem(MetricRegistryType.CloudWatch));
//    }
//
//    @Test
//    void testConfigurationWithHttpBasic() {
//        final DataPrepperConfiguration dataPrepperConfiguration =
//                DataPrepperConfiguration.fromFile(new File(VALID_DATA_PREPPER_CONFIG_FILE_WITH_BASIC_AUTHENTICATION));
//
//        assertThat(dataPrepperConfiguration, notNullValue());
//        assertThat(dataPrepperConfiguration.getAuthentication(), notNullValue());
//        assertThat(dataPrepperConfiguration.getAuthentication().getPluginName(), equalTo("http_basic"));
//        assertThat(dataPrepperConfiguration.getAuthentication().getPluginSettings(), notNullValue());
//        assertThat(dataPrepperConfiguration.getAuthentication().getPluginSettings(), hasKey("username"));
//        assertThat(dataPrepperConfiguration.getAuthentication().getPluginSettings(), hasKey("password"));
//    }
//
//    @Test
//    public void testInvalidConfig() {
//        assertThrows(IllegalArgumentException.class, () ->
//                    DataPrepperConfiguration.fromFile(new File(INVALID_DATA_PREPPER_CONFIG_FILE)));
//    }
//
//    @Test
//    public void testInvalidPortConfig() {
//        assertThrows(IllegalArgumentException.class, () ->
//                DataPrepperConfiguration.fromFile(new File(INVALID_PORT_DATA_PREPPER_CONFIG_FILE)));
//    }
}
