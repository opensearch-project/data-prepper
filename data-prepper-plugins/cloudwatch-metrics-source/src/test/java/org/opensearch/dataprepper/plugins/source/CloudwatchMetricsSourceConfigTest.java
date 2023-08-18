/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.configuration.DimensionConfig;
import org.opensearch.dataprepper.plugins.source.configuration.MetricsConfig;
import org.opensearch.dataprepper.plugins.source.configuration.NamespaceConfig;
import software.amazon.awssdk.regions.Region;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CloudwatchMetricsSourceConfigTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void cloud_watch_metrics_configuration_test() throws JsonProcessingException {

        String cloudMetricsYaml = "      aws:\n" +
                "        region: ap-south-1\n" +
                "        sts_role_arn: \"arn:aws:iam::524239988944:role/app-test\"\n" +
                "      batch_size: 2000\n" +
                "      namespaces:\n" +
                "        - namespace:\n" +
                "            name: \"AWS/S3\"\n" +
                "            start_time: \"2023-05-19T18:35:24z\"\n" +
                "            end_time: \"2023-08-07T18:35:24z\"\n" +
                "            metricDataQueries:\n" +
                "              - metric:\n" +
                "                  name: BucketSizeBytes\n" +
                "                  id: \"q1\"\n" +
                "                  period: 86400\n" +
                "                  stat: \"Average\"\n" +
                "                  unit: \"Bytes\"\n" +
                "                  dimensions:\n" +
                "                    - dimension:\n" +
                "                        name: \"StorageType\"\n" +
                "                        value: \"StandardStorage\"";
        final CloudwatchMetricsSourceConfig metricsSourceConfig = objectMapper.readValue(cloudMetricsYaml, CloudwatchMetricsSourceConfig.class);
        assertThat(metricsSourceConfig.getBatchSize(), equalTo(2000));
        assertThat(metricsSourceConfig.getAwsAuthenticationOptions().getAwsRegion(), equalTo(Region.AP_SOUTH_1));
        assertThat(metricsSourceConfig.getAwsAuthenticationOptions().getAwsStsRoleArn(), equalTo("arn:aws:iam::524239988944:role/app-test"));

        final NamespaceConfig namespaceConfig = metricsSourceConfig.getNamespacesListConfig().get(0).getNamespaceConfig();
        assertThat(namespaceConfig.getName(), equalTo("AWS/S3"));
        assertThat(namespaceConfig.getEndTime(), equalTo("2023-08-07T18:35:24z"));
        assertThat(namespaceConfig.getStartTime(), equalTo("2023-05-19T18:35:24z"));
        assertThat(namespaceConfig.getMetricNames(), nullValue());
        final MetricsConfig metricsConfig = namespaceConfig.getMetricDataQueriesConfig().get(0).getMetricsConfig();
        assertThat(metricsConfig.getName(), equalTo("BucketSizeBytes"));
        assertThat(metricsConfig.getId(), equalTo("q1"));
        assertThat(metricsConfig.getPeriod(), equalTo(86400));
        assertThat(metricsConfig.getStat(), equalTo("Average"));
        assertThat(metricsConfig.getUnit(), equalTo("Bytes"));
        final DimensionConfig dimensionConfig =
                metricsConfig.getDimensionsListConfigs().get(0).getDimensionConfig();
        assertThat(dimensionConfig.getName(), equalTo("StorageType"));
        assertThat(dimensionConfig.getValue(), equalTo("StandardStorage"));
    }

    @Test
    public void cloud_watch_default_batch_size_test(){
        assertThat(new CloudwatchMetricsSourceConfig().getBatchSize(),equalTo(1000));
    }
}